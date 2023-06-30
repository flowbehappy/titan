#include "PageHouse_table_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <V3/Universal/UniversalPageStorage.h>
#include <WriteBatchImpl.h>

#include <cinttypes>

#include "monitoring/statistics.h"
#include "titan_logging.h"

namespace rocksdb
{
namespace titandb
{

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

        auto page_id = index.file_number;
        auto value_size = index.blob_handle.size;
        auto page = pagehouse_manager_->getStore()->read(0, index.file_number, {}, {}, false);
        UpdateIOBytes(prev_bytes_read, prev_bytes_written, &io_bytes_read_, &io_bytes_written_);
        if (page.isValid())
        {
            ikey.type = kTypeValue;
            std::string index_key;
            AppendInternalKey(&index_key, ikey);

            auto buf = page.getDataWithDecompressed(value_size);
            Slice slice(buf.begin(), value_size);

            base_builder_->Add(index_key, slice);
            bytes_read_ += page.data.size();
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

    StopWatch write_sw(db_options_.env->GetSystemClock().get(), statistics(stats_), TITAN_BLOB_FILE_WRITE_MICROS);

    RecordTick(statistics(stats_), TITAN_BLOB_FILE_NUM_KEYS_WRITTEN);
    RecordInHistogram(statistics(stats_), TITAN_KEY_SIZE, record.key.size());
    RecordInHistogram(statistics(stats_), TITAN_VALUE_SIZE, record.value.size());
    AddStats(stats_, cf_id_, TitanInternalStats::LIVE_BLOB_SIZE, record.value.size());
    bytes_written_ += record.key.size() + record.value.size();

    // Write value to page storage
    auto page_id = pagehouse_manager_->getAndIncrNextPageId();
    ::DB::WriteBatch wb(0);
    wb.putPageAndCompress(page_id, 0, value.ToStringView(), pagehouse_manager_->getCompressionSettings());
    pagehouse_manager_->getStore()->write(std::move(wb));

    // Written pages could be rollbacked if Abandon() is called.
    written_pageids.emplace_back(page_id);

    UpdateIOBytes(prev_bytes_read, prev_bytes_written, &io_bytes_read_, &io_bytes_written_);

    // Write key to sst
    ParsedInternalKey new_ikey = ikey;
    new_ikey.type = kTypeBlobIndex;
    std::string new_key;
    AppendInternalKey(&new_key, new_ikey);
    base_builder_->Add(new_key, value);
}

Status PageHouseTableBuilder::status() const
{
    Status s = status_;
    if (s.ok())
    {
        s = base_builder_->status();
    }
    return s;
}

Status PageHouseTableBuilder::Finish()
{
    // `FinishBlobFile()` may transform its state from `kBuffered` to
    // `kUnbuffered`, in this case, the relative blob handles will be updated, so
    // `base_builder_->Finish()` have to be after `FinishBlobFile()`
    base_builder_->Finish();
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
    if (!written_pageids.empty())
    {
        ::DB::WriteBatch wb(0);
        for (auto id : written_pageids)
            wb.delPage(id);
        pagehouse_manager_->getStore()->write(std::move(wb));
    }
}

uint64_t PageHouseTableBuilder::NumEntries() const
{
    return base_builder_->NumEntries();
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
}

} // namespace titandb
} // namespace rocksdb
