#include "PageHouse_db_impl.h"

#include <Common/UnifiedLogFormatter.h>
#include <Poco/Exception.h>
#include <Poco/Ext/LevelFilterChannel.h>
#include <Poco/Ext/ReloadableSplitterChannel.h>
#include <Poco/Ext/SourceFilterChannel.h>
#include <Poco/Ext/TiFlashLogFileChannel.h>
#include <Poco/Formatter.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Path.h>
#include <WriteBatchImpl.h>

#include <cinttypes>

#include "PageHouse_base_db_listener.h"
#include "PageHouse_compaction_filter.h"
#include "PageHouse_db_iter.h"
#include "PageHouse_table_factory.h"
#include "db/arena_wrapped_db_iter.h"
#include "db_iter.h"
#include "logging/log_buffer.h"
#include "monitoring/statistics_impl.h"
#include "port/port.h"
#include "table_factory.h"
#include "titan_build_version.h"
#include "titan_logging.h"
#include "titan_stats.h"
#include "util/autovector.h"
#include "util/mutexlock.h"
#include "util/threadpool_imp.h"

namespace rocksdb
{
namespace titandb
{

PageHouseTitanDBImpl::PageHouseTitanDBImpl(const TitanDBOptions & options, const std::string & dbname)
    : dbname_(dbname), env_(options.env), env_options_(options), db_options_(options), thread_pool(1, "PH_BG_")
{
    if (db_options_.dirname.empty())
    {
        db_options_.dirname = dbname_ + "/pagehouse";
    }
    if (db_options_.statistics != nullptr)
    {
        // The point of `statistics` is that it can be shared by multiple instances.
        // So we should check if it's a qualified statistics instead of overwriting
        // it.
        db_options_.statistics->getTickerCount(TITAN_TICKER_ENUM_MAX - 1);
        HistogramData data;
        db_options_.statistics->histogramData(TITAN_HISTOGRAM_ENUM_MAX - 1, &data);
        stats_.reset(new TitanStats(db_options_.statistics.get()));
    }
    initPageHouseLogger();
}

PageHouseTitanDBImpl::~PageHouseTitanDBImpl()
{
    Close();

    if (log_file)
    {
        log_file->close();
        log_file = nullptr;
    }
}

void PageHouseTitanDBImpl::initPageHouseLogger()
{
    // For simplicity, we store logs in db path. Hacking only.
    if (Poco::Logger::has("___Initialized__"))
        return;
    Poco::Logger::get("___Initialized__");

    String log_path = dbname_ + "/pagehouse.log";
    Poco::AutoPtr<Poco::ReloadableSplitterChannel> split = new Poco::ReloadableSplitterChannel;
    Poco::AutoPtr<Poco::Formatter> pf = new ::DB::UnifiedLogFormatter<false>();
    Poco::AutoPtr<Poco::FormattingChannel> log = new Poco::FormattingChannel(pf);
    log_file = new Poco::TiFlashLogFileChannel;
    log_file->setProperty(Poco::FileChannel::PROP_PATH, Poco::Path(log_path).absolute().toString());
    log_file->setProperty(Poco::FileChannel::PROP_ROTATION, "100M");
    log_file->setProperty(Poco::FileChannel::PROP_TIMES, "local");
    log_file->setProperty(Poco::FileChannel::PROP_ARCHIVE, "timestamp");
    log_file->setProperty(Poco::FileChannel::PROP_COMPRESS, "true");
    log_file->setProperty(Poco::FileChannel::PROP_PURGECOUNT, "10");
    log_file->setProperty(Poco::FileChannel::PROP_FLUSH, "true");
    log_file->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, "false");
    log->setChannel(log_file);
    split->addChannel(log);
    log_file->open();

    split->open();

    Poco::Logger::root().setLevel("debug");
    Poco::Logger::root().setChannel(split);
}

Status PageHouseTitanDBImpl::Open(const std::vector<TitanCFDescriptor> & descs, std::vector<ColumnFamilyHandle *> * handles)
{
    page_manager = std::make_shared<PageHouseManager>(db_options_.dirname);
    gc_handle = thread_pool.addTask([=]() { return page_manager->triggerGC(); });

    if (handles == nullptr)
    {
        return Status::InvalidArgument("handles must be non-null.");
    }
    Status s = OpenImpl(descs, handles);
    // Cleanup after failure.
    if (!s.ok())
    {
        if (handles->size() > 0)
        {
            assert(db_ != nullptr);
            for (ColumnFamilyHandle * cfh : *handles)
            {
                Status destroy_handle_status = db_->DestroyColumnFamilyHandle(cfh);
                if (!destroy_handle_status.ok())
                {
                    TITAN_LOG_ERROR(db_options_.info_log,
                        "Failed to destroy CF handle after open failure: %s",
                        destroy_handle_status.ToString().c_str());
                }
            }
            handles->clear();
        }
        if (db_ != nullptr)
        {
            Status close_status = db_->Close();
            if (!close_status.ok())
            {
                TITAN_LOG_ERROR(db_options_.info_log, "Failed to close base DB after open failure: %s", close_status.ToString().c_str());
            }
            db_ = nullptr;
            db_impl_ = nullptr;
        }
    }
    return s;
}

Status PageHouseTitanDBImpl::OpenImpl(const std::vector<TitanCFDescriptor> & descs, std::vector<ColumnFamilyHandle *> * handles)
{
    // Setup options.
    db_options_.listeners.emplace_back(std::make_shared<PageHouseBaseDbListener>(this));
    // Descriptors for actually open DB.
    std::vector<ColumnFamilyDescriptor> base_descs;
    std::vector<std::shared_ptr<PageHouseTitanTableFactory>> titan_table_factories;
    for (auto & desc : descs)
    {
        base_descs.emplace_back(desc.name, desc.options);
        ColumnFamilyOptions & cf_opts = base_descs.back().options;
        // Disable compactions before everything is initialized.
        cf_opts.disable_auto_compactions = true;
        titan_table_factories.push_back(std::make_shared<PageHouseTitanTableFactory>(db_options_, desc.options, this, stats_.get()));
        cf_opts.table_factory = titan_table_factories.back();
        if (cf_opts.compaction_filter != nullptr || cf_opts.compaction_filter_factory != nullptr)
        {
            auto titan_cf_factory = std::make_shared<PageHouseTitanCompactionFilterFactory>(cf_opts.compaction_filter,
                cf_opts.compaction_filter_factory,
                this,
                desc.options.skip_value_in_compaction_filter,
                desc.name);
            cf_opts.compaction_filter = nullptr;
            cf_opts.compaction_filter_factory = titan_cf_factory;
        }
    }

    // Open base DB.
    auto s = DB::Open(db_options_, dbname_, base_descs, handles, &db_);
    if (!s.ok())
    {
        db_ = nullptr;
        handles->clear();
        return s;
    }
    db_impl_ = reinterpret_cast<DBImpl *>(db_->GetRootDB());
    return s;
}

Status PageHouseTitanDBImpl::Close()
{
    Status s;
    CloseImpl();
    if (db_)
    {
        s = db_->Close();
        delete db_;
        db_ = nullptr;
        db_impl_ = nullptr;
    }
    return s;
}

Status PageHouseTitanDBImpl::CloseImpl()
{
    if (gc_handle)
    {
        thread_pool.removeTask(gc_handle);
        gc_handle = nullptr;
    }

    page_manager = {};
    return Status::OK();
}

Status PageHouseTitanDBImpl::CreateColumnFamilies(const std::vector<TitanCFDescriptor> & descs, std::vector<ColumnFamilyHandle *> * handles)
{
    std::vector<ColumnFamilyDescriptor> base_descs;
    std::vector<std::shared_ptr<TableFactory>> base_table_factory;
    std::vector<std::shared_ptr<PageHouseTitanTableFactory>> titan_table_factory;
    for (auto & desc : descs)
    {
        ColumnFamilyOptions options = desc.options;
        // Replaces the provided table factory with TitanTableFactory.
        base_table_factory.emplace_back(options.table_factory);
        titan_table_factory.emplace_back(std::make_shared<PageHouseTitanTableFactory>(db_options_, desc.options, this, stats_.get()));
        options.table_factory = titan_table_factory.back();
        if (options.compaction_filter != nullptr || options.compaction_filter_factory != nullptr)
        {
            std::shared_ptr<PageHouseTitanCompactionFilterFactory> titan_cf_factory
                = std::make_shared<PageHouseTitanCompactionFilterFactory>(options.compaction_filter,
                    options.compaction_filter_factory,
                    this,
                    desc.options.skip_value_in_compaction_filter,
                    desc.name);
            options.compaction_filter = nullptr;
            options.compaction_filter_factory = titan_cf_factory;
        }

        base_descs.emplace_back(desc.name, options);
    }

    Status s = db_impl_->CreateColumnFamilies(base_descs, handles);
    assert(handles->size() == descs.size());

    if (s.ok())
    {
        std::map<uint32_t, TitanCFOptions> column_families;
        {
            MutexLock l(&mutex_);
            for (size_t i = 0; i < descs.size(); i++)
            {
                ColumnFamilyHandle * handle = (*handles)[i];
                uint32_t cf_id = handle->GetID();
                column_families.emplace(cf_id, descs[i].options);
                cf_info_.emplace(cf_id,
                    PageHouseTitanColumnFamilyInfo({handle->GetName(),
                        ImmutableTitanCFOptions(descs[i].options),
                        MutableTitanCFOptions(descs[i].options),
                        base_table_factory[i],
                        titan_table_factory[i]}));
            }
        }
    }
    if (s.ok())
    {
        for (auto & desc : descs)
        {
            TITAN_LOG_INFO(db_options_.info_log, "Created column family [%s].", desc.name.c_str());
            desc.options.Dump(db_options_.info_log.get());
        }
    }
    else
    {
        std::string column_families_str;
        for (auto & desc : descs)
        {
            column_families_str += "[" + desc.name + "]";
        }
        TITAN_LOG_ERROR(db_options_.info_log, "Failed to create column families %s: %s", column_families_str.c_str(), s.ToString().c_str());
    }
    return s;
}

Status PageHouseTitanDBImpl::DropColumnFamilies(const std::vector<ColumnFamilyHandle *> & handles)
{
    TEST_SYNC_POINT("TitanDBImpl::DropColumnFamilies:Begin");
    std::vector<uint32_t> column_families;
    std::string column_families_str;
    for (auto & handle : handles)
    {
        column_families.emplace_back(handle->GetID());
        column_families_str += "[" + handle->GetName() + "]";
    }
    {
        MutexLock l(&mutex_);
        drop_cf_requests_++;
    }
    TEST_SYNC_POINT_CALLBACK("TitanDBImpl::DropColumnFamilies:BeforeBaseDBDropCF", nullptr);
    Status s = db_impl_->DropColumnFamilies(handles);
    if (s.ok())
    {
        drop_cf_requests_--;
    }
    if (s.ok())
    {
        TITAN_LOG_INFO(db_options_.info_log, "Dropped column families: %s", column_families_str.c_str());
    }
    else
    {
        TITAN_LOG_ERROR(db_options_.info_log, "Failed to drop column families %s: %s", column_families_str.c_str(), s.ToString().c_str());
    }
    return s;
}

Status PageHouseTitanDBImpl::DestroyColumnFamilyHandle(ColumnFamilyHandle * column_family)
{
    if (column_family == nullptr)
    {
        return Status::InvalidArgument("Column family handle is nullptr.");
    }
    auto cf_id = column_family->GetID();
    std::string cf_name = column_family->GetName();
    Status s = db_impl_->DestroyColumnFamilyHandle(column_family);

    if (s.ok())
    {
        assert(cf_info_.count(cf_id) > 0);
        cf_info_.erase(cf_id);
    }
    if (s.ok())
    {
        TITAN_LOG_INFO(db_options_.info_log, "Destroyed column family handle [%s].", cf_name.c_str());
    }
    else
    {
        TITAN_LOG_ERROR(db_options_.info_log, "Failed to destroy column family handle [%s]: %s", cf_name.c_str(), s.ToString().c_str());
    }
    return s;
}

Status PageHouseTitanDBImpl::CompactFiles(const CompactionOptions & compact_options,
    ColumnFamilyHandle * column_family,
    const std::vector<std::string> & input_file_names,
    const int output_level,
    const int output_path_id,
    std::vector<std::string> * const output_file_names,
    CompactionJobInfo * compaction_job_info)
{
    std::unique_ptr<CompactionJobInfo> compaction_job_info_ptr;
    if (compaction_job_info == nullptr)
    {
        compaction_job_info_ptr.reset(new CompactionJobInfo());
        compaction_job_info = compaction_job_info_ptr.get();
    }
    auto s = db_impl_->CompactFiles(
        compact_options, column_family, input_file_names, output_level, output_path_id, output_file_names, compaction_job_info);
    if (s.ok())
    {
        OnCompactionCompleted(*compaction_job_info);
    }

    return s;
}

Status PageHouseTitanDBImpl::Put(const rocksdb::WriteOptions & options,
    rocksdb::ColumnFamilyHandle * column_family,
    const rocksdb::Slice & key,
    const rocksdb::Slice & value)
{
    return db_->Put(options, column_family, key, value);
}

Status PageHouseTitanDBImpl::Write(const rocksdb::WriteOptions & options, rocksdb::WriteBatch * updates, PostWriteCallback * callback)
{
    if (updates->HasDelete())
    {
        // Remove the actual value from blob store
        auto iter = updates->NewIterator();
        std::vector<Slice> to_del_keys;
        std::vector<ColumnFamilyHandle *> cf_handles;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next())
        {
            auto type = iter->GetValueType();
            if (ValueType::kTypeDeletion != type && ValueType::kTypeColumnFamilyDeletion != type)
                continue;
            auto cfid = iter->GetColumnFamilyId();
            auto key = iter->Key();
            ParsedInternalKey ikey;
            auto s = ParseInternalKey(key, &ikey, false /*log_err_key*/);
            if (s.ok() && ikey.type == ValueType::kTypeBlobIndex)
            {
                // Only handle the blob key type
                auto cfh = db_impl_->GetColumnFamilyHandle(cfid);

                cf_handles.emplace_back(cfh);
                to_del_keys.emplace_back(key);
            }
        }
        if (!to_del_keys.empty())
        {
            ::DB::WriteBatch wb(0);
            std::vector<std::string> values;
            auto status = this->MultiGet(ReadOptions(), cf_handles, to_del_keys, &values);
            for (size_t i = 0; i < values.size(); ++i)
            {
                auto & key = to_del_keys[i];
                Slice value(values[i]);
                if (!status[i].ok())
                {
                    TITAN_LOG_WARN(
                        db_options_.info_log, "Write Can not find key: [%s] in base db. Ignoring deletion.", key.ToString().c_str());
                    continue;
                }
                BlobIndex index;
                auto s = index.DecodeFrom(&value);
                if (!s.ok())
                    continue;
                auto page_id = index.file_number;
                wb.delPage(page_id);
            }

            // Do delete blob value
            page_manager->getStore()->write(std::move(wb));
        }
    }

    return db_->Write(options, updates, callback);
}

Status PageHouseTitanDBImpl::MultiBatchWrite(
    const WriteOptions & options, std::vector<WriteBatch *> && updates, PostWriteCallback * callback)
{
    return db_->MultiBatchWrite(options, std::move(updates), callback);
}

Status PageHouseTitanDBImpl::Delete(
    const rocksdb::WriteOptions & options, rocksdb::ColumnFamilyHandle * column_family, const rocksdb::Slice & key)
{
    return db_->Delete(options, column_family, key);
}

Status PageHouseTitanDBImpl::IngestExternalFile(rocksdb::ColumnFamilyHandle * column_family,
    const std::vector<std::string> & external_files,
    const rocksdb::IngestExternalFileOptions & options)
{
    return db_->IngestExternalFile(column_family, external_files, options);
}

Status PageHouseTitanDBImpl::CompactRange(const rocksdb::CompactRangeOptions & options,
    rocksdb::ColumnFamilyHandle * column_family,
    const rocksdb::Slice * begin,
    const rocksdb::Slice * end)
{
    return db_->CompactRange(options, column_family, begin, end);
}

Status PageHouseTitanDBImpl::Flush(const rocksdb::FlushOptions & options, rocksdb::ColumnFamilyHandle * column_family)
{
    return db_->Flush(options, column_family);
}

Status PageHouseTitanDBImpl::Get(const ReadOptions & options, ColumnFamilyHandle * handle, const Slice & key, PinnableSlice * value)
{
    if (options.snapshot)
    {
        return GetImpl(options, handle, key, value);
    }
    ReadOptions ro(options);
    ManagedSnapshot snapshot(this);
    ro.snapshot = snapshot.snapshot();
    return GetImpl(ro, handle, key, value);
}

Status PageHouseTitanDBImpl::GetImpl(const ReadOptions & options, ColumnFamilyHandle * handle, const Slice & key, PinnableSlice * value)
{
    Status s;
    bool is_blob_index = false;
    DBImpl::GetImplOptions gopts;
    gopts.column_family = handle;
    gopts.value = value;
    gopts.is_blob_index = &is_blob_index;
    s = db_impl_->GetImpl(options, key, gopts);
    if (!s.ok() || !is_blob_index)
        return s;

    StopWatch get_sw(env_->GetSystemClock().get(), statistics(stats_.get()), TITAN_GET_MICROS);
    RecordTick(statistics(stats_.get()), TITAN_NUM_GET);

    BlobIndex index;
    s = index.DecodeFrom(value);
    assert(s.ok());
    if (!s.ok())
        return s;

    auto page_id = index.file_number;
    auto value_size = index.blob_handle.size;
    auto page = page_manager->getStore()->read(0, page_id);

    char * buffer = new char[value_size];
    Slice slice(buffer, value_size);
    SCOPE_EXIT({
        if (buffer)
            delete[] buffer;
    });
    page.getDataWithDecompressed(buffer, value_size);

    static Cleanable::CleanupFunction cleanup_func = [](void * arg1, void * /*arg2*/) { delete[] reinterpret_cast<char *>(arg1); };

    value->Reset();
    value->PinSlice(slice, cleanup_func, buffer, nullptr);
    buffer = nullptr;

    return s;
}

std::vector<Status> PageHouseTitanDBImpl::MultiGet(const ReadOptions & options,
    const std::vector<ColumnFamilyHandle *> & handles,
    const std::vector<Slice> & keys,
    std::vector<std::string> * values)
{
    auto options_copy = options;
    options_copy.total_order_seek = true;
    if (options_copy.snapshot)
    {
        return MultiGetImpl(options_copy, handles, keys, values);
    }
    ReadOptions ro(options_copy);
    ManagedSnapshot snapshot(this);
    ro.snapshot = snapshot.snapshot();
    return MultiGetImpl(ro, handles, keys, values);
}

std::vector<Status> PageHouseTitanDBImpl::MultiGetImpl(const ReadOptions & options,
    const std::vector<ColumnFamilyHandle *> & handles,
    const std::vector<Slice> & keys,
    std::vector<std::string> * values)
{
    std::vector<Status> res;
    res.resize(keys.size());
    values->resize(keys.size());
    for (size_t i = 0; i < keys.size(); i++)
    {
        auto value = &(*values)[i];
        PinnableSlice pinnable_value(value);
        res[i] = GetImpl(options, handles[i], keys[i], &pinnable_value);
        if (res[i].ok() && pinnable_value.IsPinned())
        {
            value->assign(pinnable_value.data(), pinnable_value.size());
        }
    }
    return res;
}

Iterator * PageHouseTitanDBImpl::NewIterator(const TitanReadOptions & options, ColumnFamilyHandle * handle)
{
    TitanReadOptions options_copy = options;
    options_copy.total_order_seek = true;
    std::shared_ptr<ManagedSnapshot> snapshot;
    if (options_copy.snapshot)
    {
        return NewIteratorImpl(options_copy, handle, snapshot);
    }
    TitanReadOptions ro(options_copy);
    snapshot.reset(new ManagedSnapshot(this));
    ro.snapshot = snapshot->snapshot();
    return NewIteratorImpl(ro, handle, snapshot);
}

Iterator * PageHouseTitanDBImpl::NewIteratorImpl(
    const TitanReadOptions & options, ColumnFamilyHandle * handle, std::shared_ptr<ManagedSnapshot> snapshot)
{
    auto cfd = reinterpret_cast<ColumnFamilyHandleImpl *>(handle)->cfd();

    std::unique_ptr<ArenaWrappedDBIter> iter(db_impl_->NewIteratorImpl(options,
        cfd,
        options.snapshot->GetSequenceNumber(),
        nullptr /*read_callback*/,
        true /*expose_blob_index*/,
        true /*allow_refresh*/));
    return new PageHouseDBIterator( //
        options,
        page_manager->getStore(),
        snapshot,
        std::move(iter),
        env_->GetSystemClock().get(),
        stats_.get(),
        db_options_.info_log.get());
}

Status PageHouseTitanDBImpl::NewIterators(
    const TitanReadOptions & options, const std::vector<ColumnFamilyHandle *> & handles, std::vector<Iterator *> * iterators)
{
    TitanReadOptions ro(options);
    ro.total_order_seek = true;
    std::shared_ptr<ManagedSnapshot> snapshot;
    if (!ro.snapshot)
    {
        snapshot.reset(new ManagedSnapshot(this));
        ro.snapshot = snapshot->snapshot();
    }
    iterators->clear();
    iterators->reserve(handles.size());
    for (auto & handle : handles)
    {
        iterators->emplace_back(NewIteratorImpl(ro, handle, snapshot));
    }
    return Status::OK();
}

const Snapshot * PageHouseTitanDBImpl::GetSnapshot()
{
    return db_->GetSnapshot();
}

void PageHouseTitanDBImpl::ReleaseSnapshot(const Snapshot * snapshot)
{
    // TODO:
    // We can record here whether the oldest snapshot is released.
    // If not, we can just skip the next round of purging obsolete files.
    db_->ReleaseSnapshot(snapshot);
}

Status PageHouseTitanDBImpl::DisableFileDeletions()
{
    // Disable base DB file deletions.
    Status s = db_impl_->DisableFileDeletions();
    if (!s.ok())
    {
        return s;
    }
    return Status::OK();
}

Status PageHouseTitanDBImpl::EnableFileDeletions(bool force)
{
    // Enable base DB file deletions.
    Status s = db_impl_->EnableFileDeletions(force);
    if (!s.ok())
    {
        return s;
    }
    return Status::OK();
}

Status PageHouseTitanDBImpl::GetAllTitanFiles(std::vector<std::string> & files, std::vector<VersionEdit> * edits)
{
    Status s = DisableFileDeletions();
    if (!s.ok())
    {
        return s;
    }

    // Do nothing

    return EnableFileDeletions(false);
}

Status PageHouseTitanDBImpl::DeleteFilesInRanges(ColumnFamilyHandle * column_family, const RangePtr * ranges, size_t n, bool include_end)
{
    // TODO: Remove the values in page storage which keys are removed.

    Status s = db_impl_->DeleteFilesInRanges(column_family, ranges, n, include_end);
    if (!s.ok())
        return s;

    return s;
}

Status PageHouseTitanDBImpl::DeleteBlobFilesInRanges(
    ColumnFamilyHandle * column_family, const RangePtr * ranges, size_t n, bool include_end)
{
    return Status::OK();
}

Options PageHouseTitanDBImpl::GetOptions(ColumnFamilyHandle * column_family) const
{
    assert(column_family != nullptr);
    Options options = db_->GetOptions(column_family);
    uint32_t cf_id = column_family->GetID();

    MutexLock l(&mutex_);
    if (cf_info_.count(cf_id) > 0)
    {
        options.table_factory = cf_info_.at(cf_id).base_table_factory;
    }
    else
    {
        TITAN_LOG_ERROR(
            db_options_.info_log, "Failed to get original table factory for column family %s.", column_family->GetName().c_str());
        options.table_factory.reset();
    }
    return options;
}

Status PageHouseTitanDBImpl::SetOptions(
    ColumnFamilyHandle * column_family, const std::unordered_map<std::string, std::string> & new_options)
{
    Status s;
    auto opts = new_options;
    bool set_blob_run_mode = false;
    TitanBlobRunMode blob_run_mode = TitanBlobRunMode::kNormal;
    {
        auto p = opts.find("blob_run_mode");
        set_blob_run_mode = (p != opts.end());
        if (set_blob_run_mode)
        {
            const std::string & blob_run_mode_string = p->second;
            auto pm = blob_run_mode_string_map.find(blob_run_mode_string);
            if (pm == blob_run_mode_string_map.end())
            {
                return Status::InvalidArgument("No blob_run_mode defined for " + blob_run_mode_string);
            }
            else
            {
                blob_run_mode = pm->second;
                TITAN_LOG_INFO(
                    db_options_.info_log, "[%s] Set blob_run_mode: %s", column_family->GetName().c_str(), blob_run_mode_string.c_str());
            }
            opts.erase(p);
        }
    }
    if (opts.size() > 0)
    {
        s = db_->SetOptions(column_family, opts);
        if (!s.ok())
        {
            return s;
        }
    }
    // Make sure base db's SetOptions success before setting blob_run_mode.
    if (set_blob_run_mode)
    {
        uint32_t cf_id = column_family->GetID();
        {
            MutexLock l(&mutex_);
            assert(cf_info_.count(cf_id) > 0);
            auto & cf_info = cf_info_[cf_id];
            cf_info.titan_table_factory->SetBlobRunMode(blob_run_mode);
            cf_info.mutable_cf_options.blob_run_mode = blob_run_mode;
        }
    }
    return Status::OK();
}

TitanOptions PageHouseTitanDBImpl::GetTitanOptions(ColumnFamilyHandle * column_family) const
{
    assert(column_family != nullptr);
    Options base_options = GetOptions(column_family);
    TitanOptions titan_options;
    *static_cast<TitanDBOptions *>(&titan_options) = db_options_;
    *static_cast<DBOptions *>(&titan_options) = static_cast<DBOptions>(base_options);
    uint32_t cf_id = column_family->GetID();
    {
        MutexLock l(&mutex_);
        assert(cf_info_.count(cf_id) > 0);
        const auto & cf_info = cf_info_.at(cf_id);
        *static_cast<TitanCFOptions *>(&titan_options)
            = TitanCFOptions(static_cast<ColumnFamilyOptions>(base_options), cf_info.immutable_cf_options, cf_info.mutable_cf_options);
    }
    return titan_options;
}

TitanDBOptions PageHouseTitanDBImpl::GetTitanDBOptions() const
{
    // Titan db_options_ is not mutable after DB open.
    TitanDBOptions result = db_options_;
    *static_cast<DBOptions *>(&result) = db_impl_->GetDBOptions();
    return result;
}

bool PageHouseTitanDBImpl::GetProperty(ColumnFamilyHandle * column_family, const Slice & property, std::string * value)
{
    assert(column_family != nullptr);
    bool s = false;
    if (stats_.get() != nullptr)
    {
        auto stats = stats_->internal_stats(column_family->GetID());
        if (stats != nullptr)
        {
            s = stats->GetStringProperty(property, value);
        }
    }
    if (s)
    {
        return s;
    }
    else
    {
        return db_impl_->GetProperty(column_family, property, value);
    }
}

bool PageHouseTitanDBImpl::GetIntProperty(ColumnFamilyHandle * column_family, const Slice & property, uint64_t * value)
{
    assert(column_family != nullptr);
    bool s = false;
    if (stats_.get() != nullptr)
    {
        auto stats = stats_->internal_stats(column_family->GetID());
        if (stats != nullptr)
        {
            s = stats->GetIntProperty(property, value);
        }
    }
    if (s)
    {
        return s;
    }
    else
    {
        return db_impl_->GetIntProperty(column_family, property, value);
    }
}

void PageHouseTitanDBImpl::OnFlushCompleted(const FlushJobInfo & flush_job_info)
{
    TEST_SYNC_POINT("TitanDBImpl::OnFlushCompleted:Begin1");
    TEST_SYNC_POINT("TitanDBImpl::OnFlushCompleted:Begin");

    TEST_SYNC_POINT("TitanDBImpl::OnFlushCompleted:Finished");
}

void PageHouseTitanDBImpl::OnCompactionCompleted(const CompactionJobInfo & compaction_job_info)
{
    TEST_SYNC_POINT("TitanDBImpl::OnCompactionCompleted:Begin");
}

} // namespace titandb
} // namespace rocksdb
