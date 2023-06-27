#pragma once

#include <string>
#include <utility>

#include "PageHouse_db_impl.h"
#include "rocksdb/compaction_filter.h"
#include "titan_logging.h"
#include "util/mutexlock.h"

namespace rocksdb
{
namespace titandb
{

class PageHouseTitanCompactionFilter final : public CompactionFilter
{
public:
    PageHouseTitanCompactionFilter(PageHouseTitanDBImpl * db,
        const std::string & cf_name,
        const CompactionFilter * original,
        std::unique_ptr<CompactionFilter> && owned_filter,
        bool skip_value)
        : db_(db),
          cf_name_(cf_name),
          pagestore(db->getPageStore()),
          original_filter_(original),
          owned_filter_(std::move(owned_filter)),
          skip_value_(skip_value),
          filter_name_(std::string("TitanCompactionfilter.").append(original_filter_->Name()))
    {
        assert(original_filter_ != nullptr);
    }

    const char * Name() const override { return filter_name_.c_str(); }

    bool IsStackedBlobDbInternalCompactionFilter() const override { return true; }

    Decision FilterV3(int level,
        const Slice & key,
        SequenceNumber seqno,
        ValueType value_type,
        const Slice & value,
        std::string * new_value,
        std::string * skip_until) const override
    {
        if (skip_value_)
        {
            return original_filter_->FilterV3(level, key, seqno, value_type, Slice(), new_value, skip_until);
        }
        if (value_type != kBlobIndex)
        {
            return original_filter_->FilterV3(level, key, seqno, value_type, value, new_value, skip_until);
        }

        BlobIndex blob_index;
        Slice original_value(value.data());
        Status s = blob_index.DecodeFrom(&original_value);
        if (!s.ok())
        {
            TITAN_LOG_ERROR(db_->db_options_.info_log, "[%s] Unable to decode blob index", cf_name_.c_str());
            // TODO(yiwu): Better to fail the compaction as well, but current
            // compaction filter API doesn't support it.
            //            {
            //                MutexLock l(&db_->mutex_);
            //                db_->SetBGError(s);
            //            }
            // Unable to decode blob index. Keeping the value.
            return Decision::kKeep;
        }

        auto page = pagestore->read(0, blob_index.file_number);
        Slice old_value(page.data.data(), page.data.size());
        auto decision = original_filter_->FilterV3(level, key, seqno, kValue, old_value, new_value, skip_until);

        // It would be a problem if it change the value whereas the value_type
        // is still kBlobIndex. For now, just returns kKeep.
        // TODO: we should make rocksdb Filter API support changing value_type
        // assert(decision != CompactionFilter::Decision::kChangeValue);
        if (decision == Decision::kChangeValue)
        {
            decision = Decision::kKeep;
        }
        return decision;
    }

private:
    PageHouseTitanDBImpl * db_;
    const std::string cf_name_;
    const PageStoragePtr pagestore;
    const CompactionFilter * original_filter_;
    const std::unique_ptr<CompactionFilter> owned_filter_;
    bool skip_value_;
    std::string filter_name_;
};

class PageHouseTitanCompactionFilterFactory final : public CompactionFilterFactory
{
public:
    PageHouseTitanCompactionFilterFactory(const CompactionFilter * original_filter,
        std::shared_ptr<CompactionFilterFactory> original_filter_factory,
        PageHouseTitanDBImpl * db,
        bool skip_value,
        const std::string & cf_name)
        : original_filter_(original_filter),
          original_filter_factory_(original_filter_factory),
          titan_db_impl_(db),
          skip_value_(skip_value),
          cf_name_(cf_name)
    {
        assert(original_filter != nullptr || original_filter_factory != nullptr);
        if (original_filter_ != nullptr)
        {
            factory_name_ = std::string("TitanCompactionFilterFactory.").append(original_filter_->Name());
        }
        else
        {
            factory_name_ = std::string("TitanCompactionFilterFactory.").append(original_filter_factory_->Name());
        }
    }

    const char * Name() const override { return factory_name_.c_str(); }

    std::unique_ptr<CompactionFilter> CreateCompactionFilter(const CompactionFilter::Context & context) override
    {
        assert(original_filter_ != nullptr || original_filter_factory_ != nullptr);

        const CompactionFilter * original_filter = original_filter_;
        std::unique_ptr<CompactionFilter> original_filter_from_factory;
        if (original_filter == nullptr)
        {
            original_filter_from_factory = original_filter_factory_->CreateCompactionFilter(context);
            original_filter = original_filter_from_factory.get();
        }

        if (original_filter == nullptr)
        {
            return nullptr;
        }

        return std::unique_ptr<CompactionFilter>(new PageHouseTitanCompactionFilter( //
            titan_db_impl_,
            cf_name_,
            original_filter,
            std::move(original_filter_from_factory),
            skip_value_));
    }

private:
    const CompactionFilter * original_filter_;
    std::shared_ptr<CompactionFilterFactory> original_filter_factory_;
    PageHouseTitanDBImpl * titan_db_impl_;
    bool skip_value_;
    const std::string cf_name_;
    std::string factory_name_;
};

} // namespace titandb
} // namespace rocksdb
