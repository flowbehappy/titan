#include "PageHouse_base_db_listener.h"

#include "PageHouse_db_impl.h"

namespace rocksdb
{
namespace titandb
{

PageHouseBaseDbListener::PageHouseBaseDbListener(PageHouseTitanDBImpl * db) : db_impl_(db)
{
    assert(db_impl_ != nullptr);
}

PageHouseBaseDbListener::~PageHouseBaseDbListener() {}

void PageHouseBaseDbListener::OnFlushCompleted(DB * /*db*/, const FlushJobInfo & flush_job_info)
{
    db_impl_->OnFlushCompleted(flush_job_info);
}

void PageHouseBaseDbListener::OnCompactionCompleted(DB * /* db */, const CompactionJobInfo & compaction_job_info)
{
    db_impl_->OnCompactionCompleted(compaction_job_info);
}

} // namespace titandb
} // namespace rocksdb
