#pragma once

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <V3/PageStorageImpl.h>

#include <cinttypes>
#include <memory>
#include <unordered_map>

#include "blob_file_reader.h"
#include "blob_format.h"
#include "db/arena_wrapped_db_iter.h"
#include "db/db_iter.h"
#include "rocksdb/env.h"
#include "titan_logging.h"
#include "titan_stats.h"

namespace rocksdb
{
namespace titandb
{

class PageHouseDBIterator : public Iterator
{
public:
    PageHouseDBIterator( //
        const TitanReadOptions & options,
        const ::DB::PageStoragePtr & storage,
        std::shared_ptr<ManagedSnapshot> snap,
        std::unique_ptr<ArenaWrappedDBIter> iter,
        SystemClock * clock,
        TitanStats * stats,
        Logger * info_log)
        : options_(options), storage_(storage), snap_(snap), iter_(std::move(iter)), clock_(clock), stats_(stats), info_log_(info_log)
    {
    }

    ~PageHouseDBIterator() { RecordInHistogram(statistics(stats_), TITAN_ITER_TOUCH_BLOB_FILE_COUNT, files_.size()); }

    bool Valid() const override { return iter_->Valid() && status_.ok(); }

    Status status() const override
    {
        // assume volatile inner iter
        if (status_.ok())
        {
            return iter_->status();
        }
        else
        {
            return status_;
        }
    }

    void SeekToFirst() override
    {
        iter_->SeekToFirst();
        if (ShouldGetBlobValue())
        {
            StopWatch seek_sw(clock_, statistics(stats_), TITAN_SEEK_MICROS);
            GetBlobValue();
            RecordTick(statistics(stats_), TITAN_NUM_SEEK);
        }
    }

    void SeekToLast() override
    {
        iter_->SeekToLast();
        if (ShouldGetBlobValue())
        {
            StopWatch seek_sw(clock_, statistics(stats_), TITAN_SEEK_MICROS);
            GetBlobValue();
            RecordTick(statistics(stats_), TITAN_NUM_SEEK);
        }
    }

    void Seek(const Slice & target) override
    {
        iter_->Seek(target);
        if (ShouldGetBlobValue())
        {
            StopWatch seek_sw(clock_, statistics(stats_), TITAN_SEEK_MICROS);
            GetBlobValue();
            RecordTick(statistics(stats_), TITAN_NUM_SEEK);
        }
    }

    void SeekForPrev(const Slice & target) override
    {
        iter_->SeekForPrev(target);
        if (ShouldGetBlobValue())
        {
            StopWatch seek_sw(clock_, statistics(stats_), TITAN_SEEK_MICROS);
            GetBlobValue();
            RecordTick(statistics(stats_), TITAN_NUM_SEEK);
        }
    }

    void Next() override
    {
        assert(Valid());
        iter_->Next();
        if (ShouldGetBlobValue())
        {
            StopWatch next_sw(clock_, statistics(stats_), TITAN_NEXT_MICROS);
            GetBlobValue();
            RecordTick(statistics(stats_), TITAN_NUM_NEXT);
        }
    }

    void Prev() override
    {
        assert(Valid());
        iter_->Prev();
        if (ShouldGetBlobValue())
        {
            StopWatch prev_sw(clock_, statistics(stats_), TITAN_PREV_MICROS);
            GetBlobValue();
            RecordTick(statistics(stats_), TITAN_NUM_PREV);
        }
    }

    Slice key() const override
    {
        assert(Valid());
        return iter_->key();
    }

    Slice value() const override
    {
        assert(Valid() && !options_.key_only);
        if (options_.key_only)
            return Slice();
        if (!iter_->IsBlob())
            return iter_->value();
        return record_.value;
    }

    bool seqno(SequenceNumber * number) const override { return iter_->seqno(number); }

private:
    bool ShouldGetBlobValue()
    {
        if (!iter_->Valid() || !iter_->IsBlob() || options_.key_only)
        {
            status_ = iter_->status();
            return false;
        }
        return true;
    }

    void GetBlobValue()
    {
        assert(iter_->status().ok());

        BlobIndex index;
        status_ = DecodeInto(iter_->value(), &index);
        if (!status_.ok())
        {
            TITAN_LOG_ERROR(info_log_,
                "Titan iterator: failed to decode blob index %s: %s",
                iter_->value().ToString(true /*hex*/).c_str(),
                status_.ToString().c_str());
            return;
        }

        auto page_id = index.file_number;
        auto page_size = index.blob_handle.size;
        auto page = storage_->read(0, page_id, {}, {}, false);
        if (!page.isValid())
        {
            TITAN_LOG_ERROR(
                info_log_, "Titan iterator: failed to read page by id %" PRIu64 ": %s", index.file_number, status_.ToString().c_str());
            return;
        }
        auto tmp_buf = page.getDataWithDecompressed(page_size);
        cur_buf.swap(tmp_buf);

        Slice slice(cur_buf.begin(), page_size);
        record_.DecodeFrom(&slice);
        return;
    }

    Status status_;
    BlobRecord record_;
    Poco::Buffer<char> cur_buf{0};

    TitanReadOptions options_;
    ::DB::PageStoragePtr storage_;
    std::shared_ptr<ManagedSnapshot> snap_;
    std::unique_ptr<ArenaWrappedDBIter> iter_;
    std::unordered_map<uint64_t, std::unique_ptr<BlobFilePrefetcher>> files_;

    SystemClock * clock_;
    TitanStats * stats_;
    Logger * info_log_;
};

} // namespace titandb
} // namespace rocksdb
