#pragma once

#include <atomic>

#include "rocksdb/table.h"
#include "titan/options.h"
#include "titan_stats.h"

namespace rocksdb
{
namespace titandb
{

class PageHouseTitanDBImpl;

class PageHouseTitanTableFactory : public TableFactory
{
public:
    PageHouseTitanTableFactory( //
        const TitanDBOptions & db_options,
        const TitanCFOptions & cf_options,
        PageHouseTitanDBImpl * db_impl,
        TitanStats * stats)
        : db_options_(db_options),
          cf_options_(cf_options),
          blob_run_mode_(cf_options.blob_run_mode),
          base_factory_(cf_options.table_factory),
          db_impl_(db_impl),
          stats_(stats)
    {
    }

    const char * Name() const override { return "TitanTable"; }

    using TableFactory::NewTableReader;

    Status NewTableReader(const ReadOptions & ro,
        const TableReaderOptions & options,
        std::unique_ptr<RandomAccessFileReader> && file,
        uint64_t file_size,
        std::unique_ptr<TableReader> * result,
        bool prefetch_index_and_filter_in_cache = true) const override;

    TableBuilder * NewTableBuilder(const TableBuilderOptions & options, WritableFileWriter * file) const override;

    void SetBlobRunMode(TitanBlobRunMode mode) { blob_run_mode_.store(mode); }

    bool IsDeleteRangeSupported() const override { return base_factory_->IsDeleteRangeSupported(); }

private:
    const TitanDBOptions db_options_;
    const TitanCFOptions cf_options_;
    std::atomic<TitanBlobRunMode> blob_run_mode_;
    std::shared_ptr<TableFactory> base_factory_;
    PageHouseTitanDBImpl * db_impl_;
    TitanStats * stats_;
};

} // namespace titandb
} // namespace rocksdb
