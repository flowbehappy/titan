#pragma once

#include "blob_file_builder.h"
#include "blob_file_manager.h"
#include "blob_file_set.h"
#include "rocksdb/types.h"
#include "table/table_builder.h"
#include "titan/options.h"
#include "titan_stats.h"
#include "PageHouse_manger.h"

namespace rocksdb
{
namespace titandb
{


class PageHouseTableBuilder : public TableBuilder
{
public:
    PageHouseTableBuilder( //
        uint32_t cf_id,
        const TitanDBOptions & db_options,
        const TitanCFOptions & cf_options,
        std::unique_ptr<TableBuilder> base_builder,
        const PageHouseManagerPtr & pagehouse_manager,
        TitanStats * stats,
        int merge_level,
        int target_level)
        : cf_id_(cf_id),
          db_options_(db_options),
          cf_options_(cf_options),
          base_builder_(std::move(base_builder)),
          pagehouse_manager_(pagehouse_manager),
          stats_(stats),
          target_level_(target_level),
          merge_level_(merge_level)
    {
    }

    void Add(const Slice & key, const Slice & value) override;

    Status status() const override;

    Status Finish() override;

    void Abandon() override;

    uint64_t NumEntries() const override;

    uint64_t FileSize() const override;

    bool NeedCompact() const override;

    TableProperties GetTableProperties() const override;

    IOStatus io_status() const override;

    std::string GetFileChecksum() const override;

    const char * GetFileChecksumFuncName() const override;

private:
    friend class TableBuilderTest;

    bool ok() const { return status().ok(); }

    std::unique_ptr<BlobFileBuilder::BlobRecordContext> NewCachedRecordContext(const ParsedInternalKey & ikey, const Slice & value);

    void AddBlob(const ParsedInternalKey & ikey, const Slice & value);

    bool ShouldMerge(const std::shared_ptr<BlobFileMeta> & file);

    void FinishBlobFile();

    void UpdateInternalOpStats();

    Status GetBlobRecord(const BlobIndex & index, BlobRecord * record, PinnableSlice * buffer);

    Status status_;
    uint32_t cf_id_;
    TitanDBOptions db_options_;
    TitanCFOptions cf_options_;
    std::unique_ptr<TableBuilder> base_builder_;
    PageHouseManagerPtr  pagehouse_manager_;
    TitanStats * stats_;

    // target level in LSM-Tree for generated SSTs and blob files
    int target_level_;
    // with cf_options_.level_merge == true, if target_level_ is higher than or
    // equals to merge_level_, values belong to blob files which have lower level
    // than target_level_ will be merged to new blob file
    int merge_level_;

    // counters
    uint64_t bytes_read_ = 0;
    uint64_t bytes_written_ = 0;
    uint64_t io_bytes_read_ = 0;
    uint64_t io_bytes_written_ = 0;
    uint64_t gc_num_keys_relocated_ = 0;
    uint64_t gc_bytes_relocated_ = 0;
    uint64_t error_read_cnt_ = 0;
};

} // namespace titandb
} // namespace rocksdb
