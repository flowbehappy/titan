#include "PageHouse_table_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <V3/Universal/UniversalPageStorage.h>

#include <cinttypes>

#include "monitoring/statistics.h"
#include "titan_logging.h"

namespace rocksdb
{
namespace titandb
{

std::unique_ptr<BlobFileBuilder::BlobRecordContext> PageHouseTableBuilder::NewCachedRecordContext(
    const ParsedInternalKey & ikey, const Slice & value)
{
    std::unique_ptr<BlobFileBuilder::BlobRecordContext> ctx(new BlobFileBuilder::BlobRecordContext);
    AppendInternalKey(&ctx->key, ikey);
    ctx->has_value = true;
    ctx->value = value.ToString();
    return ctx;
}

void PageHouseTableBuilder::Add(const Slice & key, const Slice & value)
{
    if (!ok())
        return;

    ParsedInternalKey ikey;
    status_ = ParseInternalKey(key, &ikey, false /*log_err_key*/);
    if (!status_.ok())
    {
        return;
    }

    uint64_t prev_bytes_read = 0;
    uint64_t prev_bytes_written = 0;
    SavePrevIOBytes(&prev_bytes_read, &prev_bytes_written);

    if (ikey.type == kTypeBlobIndex && cf_options_.blob_run_mode == TitanBlobRunMode::kFallback)
    {
        // we ingest value from blob file
        Slice copy = value;
        BlobIndex index;
        status_ = index.DecodeFrom(&copy);
        if (!ok())
        {
            return;
        }

        BlobRecord record;
        PinnableSlice buffer;
        Status get_status = GetBlobRecord(index, &record, &buffer);
        UpdateIOBytes(prev_bytes_read, prev_bytes_written, &io_bytes_read_, &io_bytes_written_);
        if (get_status.ok())
        {
            ikey.type = kTypeValue;
            std::string index_key;
            AppendInternalKey(&index_key, ikey);

            base_builder_->Add(index_key, record.value);
            bytes_read_ += record.size();
        }
        else
        {
            // Get blob value can fail if corresponding blob file has been GC-ed
            // deleted. In this case we write the blob index as is to compaction
            // output.
            // TODO: return error if it is indeed an error.
            base_builder_->Add(key, value);
        }
    }
    else if (ikey.type == kTypeValue && cf_options_.blob_run_mode == TitanBlobRunMode::kNormal)
    {
        bool is_small_kv = value.size() < cf_options_.min_blob_size;
        if (is_small_kv)
        {
            base_builder_->Add(key, value);
        }
        else
        {
            // We write to blob file and insert index
            AddBlob(ikey, value);
        }
    }
    else
    {
        // Mainly processing kTypeMerge and kTypeBlobIndex in both flushing and
        // compaction.
        base_builder_->Add(key, value);
    }
}

void PageHouseTableBuilder::AddBlob(const ParsedInternalKey & ikey, const Slice & value)
{
    if (!ok())
        return;

    BlobRecord record;
    record.key = ikey.user_key;
    record.value = value;

    uint64_t prev_bytes_read = 0;
    uint64_t prev_bytes_written = 0;
    SavePrevIOBytes(&prev_bytes_read, &prev_bytes_written);

    BlobFileBuilder::OutContexts contexts;

    StopWatch write_sw(db_options_.env->GetSystemClock().get(), statistics(stats_), TITAN_BLOB_FILE_WRITE_MICROS);


    RecordTick(statistics(stats_), TITAN_BLOB_FILE_NUM_KEYS_WRITTEN);
    RecordInHistogram(statistics(stats_), TITAN_KEY_SIZE, record.key.size());
    RecordInHistogram(statistics(stats_), TITAN_VALUE_SIZE, record.value.size());
    AddStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_SIZE, record.value.size());
    bytes_written_ += record.key.size() + record.value.size();

    std::unique_ptr<BlobFileBuilder::BlobRecordContext> ctx(new BlobFileBuilder::BlobRecordContext);
    ParsedInternalKey new_ikey = ikey;
    new_ikey.type = kTypeBlobIndex;

    auto page_id = pagehouse_manager_->getAndIncrNextPageId();

    ctx->new_blob_index.file_number = page_id;

    UpdateIOBytes(prev_bytes_read, prev_bytes_written, &io_bytes_read_, &io_bytes_written_);

    std::string new_key;
    AppendInternalKey(&new_key, new_ikey);
    base_builder_->Add(new_key, value);
}

void PageHouseTableBuilder::FinishBlobFile()
{
    if (blob_builder_)
    {
        uint64_t prev_bytes_read = 0;
        uint64_t prev_bytes_written = 0;
        SavePrevIOBytes(&prev_bytes_read, &prev_bytes_written);
        Status s;
        BlobFileBuilder::OutContexts contexts;
        s = blob_builder_->Finish(&contexts);
        UpdateIOBytes(prev_bytes_read, prev_bytes_written, &io_bytes_read_, &io_bytes_written_);
        AddBlobResultsToBase(contexts);

        if (s.ok() && ok())
        {
            TITAN_LOG_INFO(db_options_.info_log, "Titan table builder finish output file %" PRIu64 ".", blob_handle_->GetNumber());
            std::shared_ptr<BlobFileMeta> file = std::make_shared<BlobFileMeta>(blob_handle_->GetNumber(),
                blob_handle_->GetFile()->GetFileSize(),
                blob_builder_->NumEntries(),
                target_level_,
                blob_builder_->GetSmallestKey(),
                blob_builder_->GetLargestKey());
            file->FileStateTransit(BlobFileMeta::FileEvent::kFlushOrCompactionOutput);
            finished_blobs_.push_back({file, std::move(blob_handle_)});
            // level merge is performed
            if (gc_num_keys_relocated_ != 0)
            {
                RecordTick(statistics(stats_), TITAN_GC_NUM_NEW_FILES, 1);
                RecordTick(statistics(stats_), TITAN_GC_NUM_KEYS_RELOCATED, gc_num_keys_relocated_);
                RecordTick(statistics(stats_), TITAN_GC_BYTES_RELOCATED, gc_bytes_relocated_);
                gc_num_keys_relocated_ = 0;
                gc_bytes_relocated_ = 0;
            }
            blob_builder_.reset();
        }
        else
        {
            TITAN_LOG_WARN(
                db_options_.info_log, "Titan table builder finish failed. Delete output file %" PRIu64 ".", blob_handle_->GetNumber());
            status_ = blob_manager_->DeleteFile(std::move(blob_handle_));
        }
    }
}

Status PageHouseTableBuilder::status() const
{
    Status s = status_;
    if (s.ok())
    {
        s = base_builder_->status();
    }
    if (s.ok() && blob_builder_)
    {
        s = blob_builder_->status();
    }
    return s;
}

Status PageHouseTableBuilder::Finish()
{
    FinishBlobFile();
    // `FinishBlobFile()` may transform its state from `kBuffered` to
    // `kUnbuffered`, in this case, the relative blob handles will be updated, so
    // `base_builder_->Finish()` have to be after `FinishBlobFile()`
    base_builder_->Finish();
    status_ = blob_manager_->BatchFinishFiles(cf_id_, finished_blobs_);
    if (!status_.ok())
    {
        TITAN_LOG_ERROR(db_options_.info_log, "Titan table builder failed on finish: %s", status_.ToString().c_str());
    }
    UpdateInternalOpStats();
    if (error_read_cnt_ > 0)
    {
        TITAN_LOG_ERROR(db_options_.info_log, "Read file error %" PRIu64 " times during level merge", error_read_cnt_);
    }
    return status();
}

void PageHouseTableBuilder::Abandon()
{
    base_builder_->Abandon();
    if (blob_builder_)
    {
        TITAN_LOG_INFO(db_options_.info_log, "Titan table builder abandoned. Delete output file %" PRIu64 ".", blob_handle_->GetNumber());
        blob_builder_->Abandon();
        status_ = blob_manager_->DeleteFile(std::move(blob_handle_));
    }
}

uint64_t PageHouseTableBuilder::NumEntries() const
{
    if (builder_unbuffered())
    {
        return base_builder_->NumEntries();
    }
    else
    {
        return blob_builder_->NumEntries() + blob_builder_->NumSampleEntries();
    }
}

uint64_t PageHouseTableBuilder::FileSize() const
{
    return base_builder_->FileSize();
}

bool PageHouseTableBuilder::NeedCompact() const
{
    return base_builder_->NeedCompact();
}

TableProperties PageHouseTableBuilder::GetTableProperties() const
{
    return base_builder_->GetTableProperties();
}

IOStatus PageHouseTableBuilder::io_status() const
{
    return base_builder_->io_status();
}

std::string PageHouseTableBuilder::GetFileChecksum() const
{
    return base_builder_->GetFileChecksum();
}

const char * PageHouseTableBuilder::GetFileChecksumFuncName() const
{
    return base_builder_->GetFileChecksumFuncName();
}

bool PageHouseTableBuilder::ShouldMerge(const std::shared_ptr<rocksdb::titandb::BlobFileMeta> & file)
{
    assert(cf_options_.level_merge);
    // Values in blob file should be merged if
    // 1. Corresponding keys are being compacted to last two level from lower
    // level
    // 2. Blob file is marked by GC or range merge
    return file != nullptr
        && (static_cast<int>(file->file_level()) < target_level_ || file->file_state() == BlobFileMeta::FileState::kToMerge);
}

void PageHouseTableBuilder::UpdateInternalOpStats()
{
    if (stats_ == nullptr)
    {
        return;
    }
    TitanInternalStats * internal_stats = stats_->internal_stats(cf_id_);
    if (internal_stats == nullptr)
    {
        return;
    }
    InternalOpType op_type = InternalOpType::COMPACTION;
    if (target_level_ == 0)
    {
        op_type = InternalOpType::FLUSH;
    }
    InternalOpStats * internal_op_stats = internal_stats->GetInternalOpStatsForType(op_type);
    assert(internal_op_stats != nullptr);
    AddStats(internal_op_stats, InternalOpStatsType::COUNT);
    AddStats(internal_op_stats, InternalOpStatsType::BYTES_READ, bytes_read_);
    AddStats(internal_op_stats, InternalOpStatsType::BYTES_WRITTEN, bytes_written_);
    AddStats(internal_op_stats, InternalOpStatsType::IO_BYTES_READ, io_bytes_read_);
    AddStats(internal_op_stats, InternalOpStatsType::IO_BYTES_WRITTEN, io_bytes_written_);
    if (blob_builder_ != nullptr)
    {
        AddStats(internal_op_stats, InternalOpStatsType::OUTPUT_FILE_NUM);
    }
}

Status PageHouseTableBuilder::GetBlobRecord(const BlobIndex & index, BlobRecord * record, PinnableSlice * buffer)
{
    Status s;

    auto it = input_file_prefetchers_.find(index.file_number);
    if (it == input_file_prefetchers_.end())
    {
        std::unique_ptr<BlobFilePrefetcher> prefetcher;
        auto storage = blob_storage_.lock();
        assert(storage != nullptr);
        s = storage->NewPrefetcher(index.file_number, &prefetcher);
        if (s.ok())
        {
            it = input_file_prefetchers_.emplace(index.file_number, std::move(prefetcher)).first;
        }
    }

    if (s.ok())
    {
        s = it->second->Get(ReadOptions(), index.blob_handle, record, buffer);
    }
    return s;
}

} // namespace titandb
} // namespace rocksdb
