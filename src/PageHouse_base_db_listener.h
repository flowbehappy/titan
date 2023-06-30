#pragma once

#include "rocksdb/listener.h"

namespace rocksdb {
namespace titandb {

class PageHouseTitanDBImpl;

class PageHouseBaseDbListener final : public EventListener {
public:
    PageHouseBaseDbListener(PageHouseTitanDBImpl* db);
    ~PageHouseBaseDbListener();

    void OnFlushCompleted(DB* db, const FlushJobInfo& flush_job_info) override;

    void OnCompactionCompleted(
        DB* db, const CompactionJobInfo& compaction_job_info) override;
private:
    rocksdb::titandb::PageHouseTitanDBImpl* db_impl_;
};

}  // namespace titandb
}  // namespace rocksdb
