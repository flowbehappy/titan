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

#include <cinttypes>

#include "PageHouse_compaction_filter.h"
#include "base_db_listener.h"
#include "blob_file_builder.h"
#include "blob_file_iterator.h"
#include "blob_file_size_collector.h"
#include "blob_gc.h"
#include "compaction_filter.h"
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
    : dbname_(dbname), env_(options.env), env_options_(options), db_options_(options)
{
    if (db_options_.dirname.empty())
    {
        db_options_.dirname = dbname_ + "/titandb";
    }
    initPageHouseLogger();
    page_manager = std::make_shared<PageHouseManager>(dbname);
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
    // Descriptors for actually open DB.
    std::vector<ColumnFamilyDescriptor> base_descs;
    std::vector<std::shared_ptr<PageHouseTitanTableFactory>> titan_table_factories;
    for (auto & desc : descs)
    {
        base_descs.emplace_back(desc.name, desc.options);
        ColumnFamilyOptions & cf_opts = base_descs.back().options;
        // Disable compactions before everything is initialized.
        cf_opts.disable_auto_compactions = true;
        cf_opts.table_properties_collector_factories.emplace_back(std::make_shared<BlobFileSizeCollectorFactory>());
        titan_table_factories.push_back(std::make_shared<PageHouseTitanTableFactory>(
            db_options_, desc.options, this, blob_manager_, &mutex_, blob_file_set_.get(), stats_.get()));
        cf_opts.table_factory = titan_table_factories.back();
        if (cf_opts.compaction_filter != nullptr || cf_opts.compaction_filter_factory != nullptr)
        {
            auto titan_cf_factory
                = std::make_shared<PageHouseTitanCompactionFilterFactory>(cf_opts.compaction_filter,
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
    return Status::OK();
}

Status TitanDBImpl::CreateColumnFamilies(const std::vector<TitanCFDescriptor> & descs, std::vector<ColumnFamilyHandle *> * handles)
{
    std::vector<ColumnFamilyDescriptor> base_descs;
    std::vector<std::shared_ptr<TableFactory>> base_table_factory;
    std::vector<std::shared_ptr<TitanTableFactory>> titan_table_factory;
    for (auto & desc : descs)
    {
        ColumnFamilyOptions options = desc.options;
        // Replaces the provided table factory with TitanTableFactory.
        base_table_factory.emplace_back(options.table_factory);
        titan_table_factory.emplace_back(std::make_shared<TitanTableFactory>(
            db_options_, desc.options, this, blob_manager_, &mutex_, blob_file_set_.get(), stats_.get()));
        options.table_factory = titan_table_factory.back();
        options.table_properties_collector_factories.emplace_back(std::make_shared<BlobFileSizeCollectorFactory>());
        if (options.compaction_filter != nullptr || options.compaction_filter_factory != nullptr)
        {
            std::shared_ptr<TitanCompactionFilterFactory> titan_cf_factory
                = std::make_shared<TitanCompactionFilterFactory>(options.compaction_filter,
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
                    TitanColumnFamilyInfo({handle->GetName(),
                        ImmutableTitanCFOptions(descs[i].options),
                        MutableTitanCFOptions(descs[i].options),
                        base_table_factory[i],
                        titan_table_factory[i]}));
            }
            blob_file_set_->AddColumnFamilies(column_families);
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

Status TitanDBImpl::DropColumnFamilies(const std::vector<ColumnFamilyHandle *> & handles)
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

        // Has to wait till no GC job is running before proceed, otherwise GC jobs
        // can fail and set background error.
        // TODO(yiwu): only wait for GC jobs of CFs being dropped.
        while (bg_gc_running_ > 0)
        {
            bg_cv_.Wait();
        }
    }
    TEST_SYNC_POINT_CALLBACK("TitanDBImpl::DropColumnFamilies:BeforeBaseDBDropCF", nullptr);
    Status s = db_impl_->DropColumnFamilies(handles);
    if (s.ok())
    {
        MutexLock l(&mutex_);
        SequenceNumber obsolete_sequence = db_impl_->GetLatestSequenceNumber();
        s = blob_file_set_->DropColumnFamilies(column_families, obsolete_sequence);
        drop_cf_requests_--;
        if (drop_cf_requests_ == 0)
        {
            bg_cv_.SignalAll();
        }
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

Status TitanDBImpl::DestroyColumnFamilyHandle(ColumnFamilyHandle * column_family)
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
        MutexLock l(&mutex_);
        // it just changes some marks and doesn't delete blob files physically.
        Status destroy_status = blob_file_set_->MaybeDestroyColumnFamily(cf_id);
        // BlobFileSet will return NotFound status if the cf is not destroyed.
        if (destroy_status.ok())
        {
            assert(cf_info_.count(cf_id) > 0);
            cf_info_.erase(cf_id);
        }
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

Status TitanDBImpl::CompactFiles(const CompactionOptions & compact_options,
    ColumnFamilyHandle * column_family,
    const std::vector<std::string> & input_file_names,
    const int output_level,
    const int output_path_id,
    std::vector<std::string> * const output_file_names,
    CompactionJobInfo * compaction_job_info)
{
    if (HasBGError())
        return GetBGError();
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

Status TitanDBImpl::Put(const rocksdb::WriteOptions & options,
    rocksdb::ColumnFamilyHandle * column_family,
    const rocksdb::Slice & key,
    const rocksdb::Slice & value)
{
    return HasBGError() ? GetBGError() : db_->Put(options, column_family, key, value);
}

Status TitanDBImpl::Write(const rocksdb::WriteOptions & options, rocksdb::WriteBatch * updates, PostWriteCallback * callback)
{
    return HasBGError() ? GetBGError() : db_->Write(options, updates, callback);
}

Status TitanDBImpl::MultiBatchWrite(const WriteOptions & options, std::vector<WriteBatch *> && updates, PostWriteCallback * callback)
{
    return HasBGError() ? GetBGError() : db_->MultiBatchWrite(options, std::move(updates), callback);
}

Status TitanDBImpl::Delete(const rocksdb::WriteOptions & options, rocksdb::ColumnFamilyHandle * column_family, const rocksdb::Slice & key)
{
    return HasBGError() ? GetBGError() : db_->Delete(options, column_family, key);
}

Status TitanDBImpl::IngestExternalFile(rocksdb::ColumnFamilyHandle * column_family,
    const std::vector<std::string> & external_files,
    const rocksdb::IngestExternalFileOptions & options)
{
    return HasBGError() ? GetBGError() : db_->IngestExternalFile(column_family, external_files, options);
}

Status TitanDBImpl::CompactRange(const rocksdb::CompactRangeOptions & options,
    rocksdb::ColumnFamilyHandle * column_family,
    const rocksdb::Slice * begin,
    const rocksdb::Slice * end)
{
    return HasBGError() ? GetBGError() : db_->CompactRange(options, column_family, begin, end);
}

Status TitanDBImpl::Flush(const rocksdb::FlushOptions & options, rocksdb::ColumnFamilyHandle * column_family)
{
    return HasBGError() ? GetBGError() : db_->Flush(options, column_family);
}

Status TitanDBImpl::Get(const ReadOptions & options, ColumnFamilyHandle * handle, const Slice & key, PinnableSlice * value)
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

Status TitanDBImpl::GetImpl(const ReadOptions & options, ColumnFamilyHandle * handle, const Slice & key, PinnableSlice * value)
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

    BlobRecord record;
    PinnableSlice buffer;

    mutex_.Lock();
    auto storage = blob_file_set_->GetBlobStorage(handle->GetID()).lock();
    mutex_.Unlock();

    if (storage)
    {
        StopWatch read_sw(env_->GetSystemClock().get(), statistics(stats_.get()), TITAN_BLOB_FILE_READ_MICROS);
        s = storage->Get(options, index, &record, &buffer);
        RecordTick(statistics(stats_.get()), TITAN_BLOB_FILE_NUM_KEYS_READ);
        RecordTick(statistics(stats_.get()), TITAN_BLOB_FILE_BYTES_READ, index.blob_handle.size);
    }
    else
    {
        TITAN_LOG_ERROR(db_options_.info_log, "Column family id:%" PRIu32 " not Found.", handle->GetID());
        return Status::NotFound("Column family id: " + std::to_string(handle->GetID()) + " not Found.");
    }
    if (s.IsCorruption())
    {
        TITAN_LOG_ERROR(db_options_.info_log,
            "Key:%s Snapshot:%" PRIu64 " GetBlobFile err:%s\n",
            key.ToString(true).c_str(),
            options.snapshot->GetSequenceNumber(),
            s.ToString().c_str());
    }
    if (s.ok())
    {
        value->Reset();
        value->PinSelf(record.value);
    }
    return s;
}

std::vector<Status> TitanDBImpl::MultiGet(const ReadOptions & options,
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

std::vector<Status> TitanDBImpl::MultiGetImpl(const ReadOptions & options,
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

Iterator * TitanDBImpl::NewIterator(const TitanReadOptions & options, ColumnFamilyHandle * handle)
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

Iterator * TitanDBImpl::NewIteratorImpl(
    const TitanReadOptions & options, ColumnFamilyHandle * handle, std::shared_ptr<ManagedSnapshot> snapshot)
{
    auto cfd = reinterpret_cast<ColumnFamilyHandleImpl *>(handle)->cfd();

    mutex_.Lock();
    auto storage = blob_file_set_->GetBlobStorage(handle->GetID()).lock();
    mutex_.Unlock();

    if (!storage)
    {
        TITAN_LOG_ERROR(db_options_.info_log, "Column family id:%" PRIu32 " not Found.", handle->GetID());
        return nullptr;
    }

    std::unique_ptr<ArenaWrappedDBIter> iter(db_impl_->NewIteratorImpl(options,
        cfd,
        options.snapshot->GetSequenceNumber(),
        nullptr /*read_callback*/,
        true /*expose_blob_index*/,
        true /*allow_refresh*/));
    return new TitanDBIterator(
        options, storage.get(), snapshot, std::move(iter), env_->GetSystemClock().get(), stats_.get(), db_options_.info_log.get());
}

Status TitanDBImpl::NewIterators(
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

const Snapshot * TitanDBImpl::GetSnapshot()
{
    return db_->GetSnapshot();
}

void TitanDBImpl::ReleaseSnapshot(const Snapshot * snapshot)
{
    // TODO:
    // We can record here whether the oldest snapshot is released.
    // If not, we can just skip the next round of purging obsolete files.
    db_->ReleaseSnapshot(snapshot);
}

Status TitanDBImpl::DisableFileDeletions()
{
    // Disable base DB file deletions.
    Status s = db_impl_->DisableFileDeletions();
    if (!s.ok())
    {
        return s;
    }

    int count = 0;
    {
        // Hold delete_titandb_file_mutex_ to make sure no
        // PurgeObsoleteFiles job is running.
        MutexLock l(&delete_titandb_file_mutex_);
        count = ++disable_titandb_file_deletions_;
    }

    TITAN_LOG_INFO(db_options_.info_log, "Disalbed blob file deletions. count: %d", count);
    return Status::OK();
}

Status TitanDBImpl::EnableFileDeletions(bool force)
{
    // Enable base DB file deletions.
    Status s = db_impl_->EnableFileDeletions(force);
    if (!s.ok())
    {
        return s;
    }

    int count = 0;
    {
        MutexLock l(&delete_titandb_file_mutex_);
        if (force)
        {
            disable_titandb_file_deletions_ = 0;
        }
        else if (disable_titandb_file_deletions_ > 0)
        {
            count = --disable_titandb_file_deletions_;
        }
        assert(count >= 0);
    }

    TITAN_LOG_INFO(db_options_.info_log, "Enabled blob file deletions. count: %d", count);
    return Status::OK();
}

Status TitanDBImpl::GetAllTitanFiles(std::vector<std::string> & files, std::vector<VersionEdit> * edits)
{
    Status s = DisableFileDeletions();
    if (!s.ok())
    {
        return s;
    }

    {
        MutexLock l(&mutex_);
        blob_file_set_->GetAllFiles(&files, edits);
    }

    return EnableFileDeletions(false);
}

Status TitanDBImpl::DeleteFilesInRanges(ColumnFamilyHandle * column_family, const RangePtr * ranges, size_t n, bool include_end)
{
    TablePropertiesCollection props;
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl *>(column_family);
    auto cfd = cfh->cfd();
    Version * version = nullptr;

    // Increment the ref count
    {
        InstrumentedMutexLock l(db_impl_->mutex());
        version = cfd->current();
        version->Ref();
    }

    auto * vstorage = version->storage_info();
    for (size_t i = 0; i < n; i++)
    {
        auto begin = ranges[i].start, end = ranges[i].limit;
        // Get all the files within range except L0, cause `DeleteFilesInRanges`
        // would not delete the files in L0.
        for (int level = 1; level < vstorage->num_non_empty_levels(); level++)
        {
            if (vstorage->LevelFiles(level).empty() || !vstorage->OverlapInLevel(level, begin, end))
            {
                continue;
            }
            std::vector<FileMetaData *> level_files;
            InternalKey begin_storage, end_storage, *begin_key, *end_key;
            if (begin == nullptr)
            {
                begin_key = nullptr;
            }
            else
            {
                begin_storage.SetMinPossibleForUserKey(*begin);
                begin_key = &begin_storage;
            }
            if (end == nullptr)
            {
                end_key = nullptr;
            }
            else
            {
                end_storage.SetMaxPossibleForUserKey(*end);
                end_key = &end_storage;
            }

            std::vector<FileMetaData *> files;
            vstorage->GetCleanInputsWithinInterval(level, begin_key, end_key, &files, -1 /* hint_index */, nullptr /* file_index */);
            for (const auto & file_meta : files)
            {
                if (file_meta->being_compacted)
                {
                    continue;
                }
                if (!include_end && end != nullptr && cfd->user_comparator()->Compare(file_meta->largest.user_key(), *end) == 0)
                {
                    continue;
                }
                auto fname = TableFileName(cfd->ioptions()->cf_paths, file_meta->fd.GetNumber(), file_meta->fd.GetPathId());
                if (props.count(fname) == 0)
                {
                    std::shared_ptr<const TableProperties> table_properties;
                    Status s = version->GetTableProperties(&table_properties, file_meta, &fname);
                    if (s.ok() && table_properties)
                    {
                        props.insert({fname, table_properties});
                    }
                    else
                    {
                        return s;
                    }
                }
            }
        }
    }

    // Decrement the ref count
    {
        InstrumentedMutexLock l(db_impl_->mutex());
        version->Unref();
    }

    auto cf_id = column_family->GetID();
    std::map<uint64_t, int64_t> blob_file_size_diff;
    for (auto & prop : props)
    {
        Status gc_stats_status = ExtractGCStatsFromTableProperty(prop.second, false /*to_add*/, &blob_file_size_diff);
        if (!gc_stats_status.ok())
        {
            // TODO: Should treat it as background error and make DB read-only.
            TITAN_LOG_ERROR(db_options_.info_log,
                "failed to extract GC stats, file: %s, error: %s",
                prop.first.c_str(),
                gc_stats_status.ToString().c_str());
            assert(false);
        }
    }

    // Here could be a running compaction install a new version after obtain
    // current and before we call DeleteFilesInRange for the base DB. In this case
    // the properties we get could be inaccurate.
    // TODO: we can use the OnTableFileDeleted callback after adding table
    // property field to TableFileDeletionInfo.
    Status s = db_impl_->DeleteFilesInRanges(column_family, ranges, n, include_end);
    if (!s.ok())
        return s;

    MutexLock l(&mutex_);
    auto bs = blob_file_set_->GetBlobStorage(cf_id).lock();
    if (!bs)
    {
        // TODO: Should treat it as background error and make DB read-only.
        TITAN_LOG_ERROR(db_options_.info_log, "Column family id:%" PRIu32 " not Found.", cf_id);
        return Status::NotFound("Column family id: " + std::to_string(cf_id) + " not Found.");
    }

    VersionEdit edit;
    auto cf_options = bs->cf_options();
    for (const auto & file_size : blob_file_size_diff)
    {
        uint64_t file_number = file_size.first;
        int64_t delta = file_size.second;
        auto file = bs->FindFile(file_number).lock();
        if (!file || file->is_obsolete())
        {
            // file has been gc out
            continue;
        }
        auto before = file->GetDiscardableRatioLevel();
        bool ok = file->UpdateLiveDataSize(delta);
        if (!ok)
        {
            TITAN_LOG_WARN(db_options_.info_log, "During DeleteFilesInRanges: blob file %" PRIu64 " live size below zero.", file_number);
            assert(false);
        }
        auto after = file->GetDiscardableRatioLevel();
        if (before != after)
        {
            AddStats(stats_.get(), cf_id, after, 1);
            SubStats(stats_.get(), cf_id, before, 1);
        }
        if (cf_options.level_merge)
        {
            if (file->NoLiveData())
            {
                RecordTick(statistics(stats_.get()), TITAN_GC_NUM_FILES, 1);
                RecordTick(statistics(stats_.get()), TITAN_GC_LEVEL_MERGE_DELETE, 1);
                edit.DeleteBlobFile(file->file_number(), db_impl_->GetLatestSequenceNumber());
            }
            else if (file->GetDiscardableRatio() > cf_options.blob_file_discardable_ratio)
            {
                RecordTick(statistics(stats_.get()), TITAN_GC_LEVEL_MERGE_MARK, 1);
                file->FileStateTransit(BlobFileMeta::FileEvent::kNeedMerge);
            }
        }
        SubStats(stats_.get(), cf_id, TitanInternalStats::LIVE_BLOB_SIZE, -delta);
    }
    if (cf_options.level_merge)
    {
        blob_file_set_->LogAndApply(edit);
    }
    else
    {
        bs->ComputeGCScore();

        AddToGCQueue(cf_id);
        MaybeScheduleGC();
    }

    return s;
}

Status TitanDBImpl::DeleteBlobFilesInRanges(ColumnFamilyHandle * column_family, const RangePtr * ranges, size_t n, bool include_end)
{
    MutexLock l(&mutex_);
    SequenceNumber obsolete_sequence = db_impl_->GetLatestSequenceNumber();
    Status s = blob_file_set_->DeleteBlobFilesInRanges(column_family->GetID(), ranges, n, include_end, obsolete_sequence);
    return s;
}

void TitanDBImpl::MarkFileIfNeedMerge(const std::vector<std::shared_ptr<BlobFileMeta>> & files, int max_sorted_runs)
{
    mutex_.AssertHeld();
    if (files.empty())
        return;

    // store and sort both ends of blob files to count sorted runs
    std::vector<std::pair<BlobFileMeta *, bool /* is smallest end? */>> blob_ends;
    for (const auto & file : files)
    {
        blob_ends.emplace_back(std::make_pair(file.get(), true));
        blob_ends.emplace_back(std::make_pair(file.get(), false));
    }
    auto blob_ends_cmp = [](const std::pair<BlobFileMeta *, bool> & end1, const std::pair<BlobFileMeta *, bool> & end2) {
        const std::string & key1 = end1.second ? end1.first->smallest_key() : end1.first->largest_key();
        const std::string & key2 = end2.second ? end2.first->smallest_key() : end2.first->largest_key();
        int cmp = key1.compare(key2);
        // when the key being the same, order largest_key before smallest_key
        return (cmp == 0) ? (!end1.second && end2.second) : (cmp < 0);
    };
    std::sort(blob_ends.begin(), blob_ends.end(), blob_ends_cmp);

    std::unordered_set<BlobFileMeta *> set;
    for (auto & end : blob_ends)
    {
        if (end.second)
        {
            set.insert(end.first);
            if (set.size() > static_cast<size_t>(max_sorted_runs))
            {
                for (auto file : set)
                {
                    RecordTick(statistics(stats_.get()), TITAN_GC_LEVEL_MERGE_MARK, 1);
                    file->FileStateTransit(BlobFileMeta::FileEvent::kNeedMerge);
                }
            }
        }
        else
        {
            set.erase(end.first);
        }
    }
}

Options TitanDBImpl::GetOptions(ColumnFamilyHandle * column_family) const
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

Status TitanDBImpl::SetOptions(ColumnFamilyHandle * column_family, const std::unordered_map<std::string, std::string> & new_options)
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
            TitanColumnFamilyInfo & cf_info = cf_info_[cf_id];
            cf_info.titan_table_factory->SetBlobRunMode(blob_run_mode);
            cf_info.mutable_cf_options.blob_run_mode = blob_run_mode;
            auto bs = blob_file_set_->GetBlobStorage(cf_id).lock();
            if (bs != nullptr)
            {
                bs->SetBlobRunMode(blob_run_mode);
            }
        }
    }
    return Status::OK();
}

TitanOptions TitanDBImpl::GetTitanOptions(ColumnFamilyHandle * column_family) const
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
        const TitanColumnFamilyInfo & cf_info = cf_info_.at(cf_id);
        *static_cast<TitanCFOptions *>(&titan_options)
            = TitanCFOptions(static_cast<ColumnFamilyOptions>(base_options), cf_info.immutable_cf_options, cf_info.mutable_cf_options);
    }
    return titan_options;
}

TitanDBOptions TitanDBImpl::GetTitanDBOptions() const
{
    // Titan db_options_ is not mutable after DB open.
    TitanDBOptions result = db_options_;
    *static_cast<DBOptions *>(&result) = db_impl_->GetDBOptions();
    return result;
}

bool TitanDBImpl::GetProperty(ColumnFamilyHandle * column_family, const Slice & property, std::string * value)
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

bool TitanDBImpl::GetIntProperty(ColumnFamilyHandle * column_family, const Slice & property, uint64_t * value)
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

void TitanDBImpl::OnFlushCompleted(const FlushJobInfo & flush_job_info)
{
    TEST_SYNC_POINT("TitanDBImpl::OnFlushCompleted:Begin1");
    TEST_SYNC_POINT("TitanDBImpl::OnFlushCompleted:Begin");
    if (!initialized())
    {
        assert(false);
        return;
    }
    std::map<uint64_t, int64_t> blob_file_size_diff;
    Status s = ExtractGCStatsFromTableProperty(flush_job_info.table_properties, true /*to_add*/, &blob_file_size_diff);
    if (!s.ok())
    {
        // TODO: Should treat it as background error and make DB read-only.
        TITAN_LOG_ERROR(
            db_options_.info_log, "OnFlushCompleted[%d]: failed to extract GC stats: %s", flush_job_info.job_id, s.ToString().c_str());
        assert(false);
    }

    {
        MutexLock l(&mutex_);
        auto blob_storage = blob_file_set_->GetBlobStorage(flush_job_info.cf_id).lock();
        if (!blob_storage)
        {
            // TODO: Should treat it as background error and make DB read-only.
            TITAN_LOG_ERROR(db_options_.info_log,
                "OnFlushCompleted[%d]: Column family id: %" PRIu32 " Not Found.",
                flush_job_info.job_id,
                flush_job_info.cf_id);
            assert(false);
            return;
        }
        for (const auto & file_diff : blob_file_size_diff)
        {
            uint64_t file_number = file_diff.first;
            int64_t delta = file_diff.second;
            auto file = blob_storage->FindFile(file_number).lock();
            // This file may be output of a GC job, and it's been GCed out.
            if (file == nullptr)
            {
                continue;
            }
            if (file->file_state() != BlobFileMeta::FileState::kPendingLSM)
            {
                // This file may be output of a GC job.
                TITAN_LOG_INFO(db_options_.info_log,
                    "OnFlushCompleted[%d]: ignore GC output file %" PRIu64 ".",
                    flush_job_info.job_id,
                    file->file_number());
                continue;
            }
            if (delta < 0)
            {
                // Cannot happen..
                TITAN_LOG_WARN(db_options_.info_log,
                    "OnFlushCompleted[%d]: New blob file %" PRIu64 " live size being negative",
                    flush_job_info.job_id,
                    file_number);
                assert(false);
                delta = 0;
            }

            if (file->live_data_size() != 0)
            {
                // Because the flushed SST is added to superversion before
                // `OnFlushCompleted()` is called, so if there is a concurrent
                // compaction, `OnCompactionCompleted()` maybe called before
                // `OnFlushCompleted()` is called.
                // In this case, the state of the blob file generated by the flush is
                // still `kPendingLSM`, while the blob file size delta is for the
                // compaction event. So it is possible that delta is negative.
                // It records the delta as a positive number if any, so here subtract it
                // from the total live data size.
                delta -= file->live_data_size();
            }
            file->set_live_data_size(static_cast<uint64_t>(delta));
            AddStats(stats_.get(), flush_job_info.cf_id, file->GetDiscardableRatioLevel(), 1);
            file->FileStateTransit(BlobFileMeta::FileEvent::kFlushCompleted);
            TITAN_LOG_INFO(db_options_.info_log,
                "OnFlushCompleted[%d]: output blob file %" PRIu64 ","
                " live data size %" PRIu64 ".",
                flush_job_info.job_id,
                file->file_number(),
                file->live_data_size());
        }
    }
    TEST_SYNC_POINT("TitanDBImpl::OnFlushCompleted:Finished");
}

void TitanDBImpl::OnCompactionCompleted(const CompactionJobInfo & compaction_job_info)
{
    TEST_SYNC_POINT("TitanDBImpl::OnCompactionCompleted:Begin");
    if (!initialized())
    {
        assert(false);
        return;
    }
    if (!compaction_job_info.status.ok())
    {
        // TODO: Clean up blob file generated by the failed compaction.
        return;
    }
    std::map<uint64_t, int64_t> blob_file_size_diff;
    const TablePropertiesCollection & prop_collection = compaction_job_info.table_properties;
    auto update_diff = [&](const std::vector<std::string> & files, bool to_add) {
        for (const auto & file_name : files)
        {
            auto prop_iter = prop_collection.find(file_name);
            if (prop_iter == prop_collection.end())
            {
                TITAN_LOG_WARN(db_options_.info_log,
                    "OnCompactionCompleted[%d]: No table properties for file %s.",
                    compaction_job_info.job_id,
                    file_name.c_str());
                continue;
            }
            Status gc_stats_status = ExtractGCStatsFromTableProperty(prop_iter->second, to_add, &blob_file_size_diff);
            if (!gc_stats_status.ok())
            {
                // TODO: Should treat it as background error and make DB read-only.
                TITAN_LOG_ERROR(db_options_.info_log,
                    "OnCompactionCompleted[%d]: failed to extract GC stats from table "
                    "property: compaction file: %s, error: %s",
                    compaction_job_info.job_id,
                    file_name.c_str(),
                    gc_stats_status.ToString().c_str());
                assert(false);
            }
        }
    };
    update_diff(compaction_job_info.input_files, false /*to_add*/);
    update_diff(compaction_job_info.output_files, true /*to_add*/);

    {
        MutexLock l(&mutex_);
        auto bs = blob_file_set_->GetBlobStorage(compaction_job_info.cf_id).lock();
        if (!bs)
        {
            // TODO: Should treat it as background error and make DB read-only.
            TITAN_LOG_ERROR(db_options_.info_log,
                "OnCompactionCompleted[%d] Column family id:%" PRIu32 " not Found.",
                compaction_job_info.job_id,
                compaction_job_info.cf_id);
            return;
        }

        VersionEdit edit;
        auto cf_options = bs->cf_options();
        std::vector<std::shared_ptr<BlobFileMeta>> to_merge_candidates;
        bool count_sorted_run
            = cf_options.level_merge && cf_options.range_merge && cf_options.num_levels - 1 == compaction_job_info.output_level;

        for (const auto & file_diff : blob_file_size_diff)
        {
            uint64_t file_number = file_diff.first;
            int64_t delta = file_diff.second;
            std::shared_ptr<BlobFileMeta> file = bs->FindFile(file_number).lock();
            if (file == nullptr || file->is_obsolete())
            {
                // File has been GC out.
                continue;
            }
            if (file->file_state() == BlobFileMeta::FileState::kPendingLSM)
            {
                if (delta <= 0)
                {
                    // Because the new generated SST is added to superversion before
                    // `OnFlushCompleted()`/`OnCompactionCompleted()` is called, so if
                    // there is a later compaction trigger by the new generated SST, the
                    // later `OnCompactionCompleted()` maybe called before the previous
                    // events' `OnFlushCompleted()`/`OnCompactionCompleted()` is called.
                    // In this case, the state of the blob file generated by the
                    // flush/compaction is still `kPendingLSM`, while the blob file size
                    // delta is for the later compaction event. So it is possible that
                    // delta is negative.
                    // To make the live data size accurate, here records the delta as a
                    // positive number. And the delta will be subtracted with total live
                    // data size in the previous
                    // `OnFlushCompleted()`/`OnCompactionCompleted()`.
                    bool ok = file->UpdateLiveDataSize(static_cast<uint64_t>(-delta));
                    if (!ok)
                    {
                        // Cannot happen
                        TITAN_LOG_WARN(db_options_.info_log,
                            "OnCompactionCompleted[%d]: pendingLSM Blob file %" PRIu64 " live size below zero.",
                            compaction_job_info.job_id,
                            file_number);
                        assert(false);
                    }
                    TITAN_LOG_INFO(db_options_.info_log,
                        "OnCompactionCompleted[%d]: Get blob file %" PRIu64 " live size being negative, maybe due to "
                        "OnFlushCompleted() is called yet",
                        compaction_job_info.job_id,
                        file_number);
                    continue;
                }
                if (file->live_data_size() != 0)
                {
                    // It records the delta as a positive number if any later compaction
                    // is trigger before previous `OnCompactionCompleted()` is called, so
                    // here subtract it from the total live data size.
                    delta -= file->live_data_size();
                }
                file->set_live_data_size(static_cast<uint64_t>(delta));
                AddStats(stats_.get(), compaction_job_info.cf_id, file->GetDiscardableRatioLevel(), 1);
                file->FileStateTransit(BlobFileMeta::FileEvent::kCompactionCompleted);
                if (file->NoLiveData())
                {
                    RecordTick(statistics(stats_.get()), TITAN_GC_NUM_FILES, 1);
                    RecordTick(statistics(stats_.get()), TITAN_GC_LEVEL_MERGE_DELETE, 1);
                    edit.DeleteBlobFile(file->file_number(), db_impl_->GetLatestSequenceNumber());
                }
                to_merge_candidates.push_back(file);
                TITAN_LOG_INFO(db_options_.info_log,
                    "OnCompactionCompleted[%d]: compaction output blob file %" PRIu64 ", live data size %" PRIu64
                    ", discardable ratio %lf.",
                    compaction_job_info.job_id,
                    file->file_number(),
                    file->live_data_size(),
                    file->GetDiscardableRatio());
            }
            else if (file->file_state() == BlobFileMeta::FileState::kNormal || file->file_state() == BlobFileMeta::FileState::kToMerge)
            {
                if (delta > 0)
                {
                    assert(false);
                    TITAN_LOG_WARN(db_options_.info_log,
                        "OnCompactionCompleted[%d]: Blob file %" PRIu64 " live size increase after compaction.",
                        compaction_job_info.job_id,
                        file_number);
                }
                auto before = file->GetDiscardableRatioLevel();
                SubStats(stats_.get(), compaction_job_info.cf_id, before, 1);
                bool ok = file->UpdateLiveDataSize(delta);
                if (!ok)
                {
                    TITAN_LOG_WARN(db_options_.info_log,
                        "OnCompactionCompleted[%d]: Blob file %" PRIu64 " live size below zero.",
                        compaction_job_info.job_id,
                        file_number);
                    assert(false);
                }
                SubStats(stats_.get(), compaction_job_info.cf_id, TitanInternalStats::LIVE_BLOB_SIZE, delta);
                if (cf_options.level_merge)
                {
                    // After level merge, most entries of merged blob files are written to
                    // new blob files. Delete blob files which have no live data.
                    // Mark last two level blob files to merge in next compaction if
                    // discardable size reached GC threshold
                    if (file->NoLiveData())
                    {
                        RecordTick(statistics(stats_.get()), TITAN_GC_NUM_FILES, 1);
                        RecordTick(statistics(stats_.get()), TITAN_GC_LEVEL_MERGE_DELETE, 1);
                        edit.DeleteBlobFile(file->file_number(), db_impl_->GetLatestSequenceNumber());
                    }
                    else if (static_cast<int>(file->file_level()) >= cf_options.num_levels - 2
                        && file->GetDiscardableRatio() > cf_options.blob_file_discardable_ratio)
                    {
                        RecordTick(statistics(stats_.get()), TITAN_GC_LEVEL_MERGE_MARK, 1);
                        file->FileStateTransit(BlobFileMeta::FileEvent::kNeedMerge);
                    }
                    else if (count_sorted_run)
                    {
                        to_merge_candidates.push_back(file);
                    }
                }
            }
            if (file->file_state() == BlobFileMeta::FileState::kNormal || file->file_state() == BlobFileMeta::FileState::kToMerge)
            {
                auto after = file->GetDiscardableRatioLevel();
                AddStats(stats_.get(), compaction_job_info.cf_id, after, 1);
            }
        }
        // If level merge is enabled, blob files will be deleted by live
        // data based GC, so we don't need to trigger regular GC anymore
        if (cf_options.level_merge)
        {
            blob_file_set_->LogAndApply(edit);
            MarkFileIfNeedMerge(to_merge_candidates, cf_options.max_sorted_runs);
        }
        else
        {
            bs->ComputeGCScore();
            AddToGCQueue(compaction_job_info.cf_id);
            MaybeScheduleGC();
        }
    }
}

Status TitanDBImpl::SetBGError(const Status & s)
{
    if (s.ok())
        return s;
    mutex_.AssertHeld();
    Status bg_err = s;
    if (!db_options_.listeners.empty())
    {
        // TODO(@jiayu) : check if mutex_ is freeable for future use case
        mutex_.Unlock();
        for (auto & listener : db_options_.listeners)
        {
            listener->OnBackgroundError(BackgroundErrorReason::kCompaction, &bg_err);
        }
        mutex_.Lock();
    }
    if (!bg_err.ok())
    {
        bg_error_ = bg_err;
        has_bg_error_.store(true);
    }
    return bg_err;
}

void TitanDBImpl::DumpStats()
{
    if (stats_ == nullptr)
    {
        return;
    }
    LogBuffer log_buffer(InfoLogLevel::HEADER_LEVEL, db_options_.info_log.get());
    {
        MutexLock l(&mutex_);
        for (auto & cf : cf_info_)
        {
            TitanInternalStats * internal_stats = stats_->internal_stats(cf.first);
            if (internal_stats == nullptr)
            {
                TITAN_LOG_WARN(db_options_.info_log, "Column family [%s] missing internal stats.", cf.second.name.c_str());
                continue;
            }
            LogToBuffer(&log_buffer, "Titan internal stats for column family [%s]:", cf.second.name.c_str());
            internal_stats->DumpAndResetInternalOpStats(&log_buffer);
        }
    }
    log_buffer.FlushBufferToLog();
}

} // namespace titandb
} // namespace rocksdb
