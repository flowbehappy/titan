#include "PageHouse_table_factory.h"

#include "PageHouse_db_impl.h"
#include "PageHouse_table_builder.h"

namespace rocksdb
{
namespace titandb
{

Status PageHouseTitanTableFactory::NewTableReader(const ReadOptions & ro,
    const TableReaderOptions & options,
    std::unique_ptr<RandomAccessFileReader> && file,
    uint64_t file_size,
    std::unique_ptr<TableReader> * result,
    bool prefetch_index_and_filter_in_cache) const
{
    return base_factory_->NewTableReader(ro, options, std::move(file), file_size, result, prefetch_index_and_filter_in_cache);
}

TableBuilder * PageHouseTitanTableFactory::NewTableBuilder(const TableBuilderOptions & options, WritableFileWriter * file) const
{
    std::unique_ptr<TableBuilder> base_builder(base_factory_->NewTableBuilder(options, file));
    TitanCFOptions cf_options = cf_options_;
    cf_options.blob_run_mode = blob_run_mode_.load();

    return new PageHouseTableBuilder(options.column_family_id,
        db_options_,
        cf_options,
        std::move(base_builder),
        db_impl_->getPageManager(),
        stats_,
        options.level_at_creation);
}

} // namespace titandb
} // namespace rocksdb
