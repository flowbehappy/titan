#pragma once

#include <Common/logger_useful.h>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/NumberFormatter.h>
#include <Poco/SyslogChannel.h>

#include "PageHouse_manger.h"
#include "blob_file_manager.h"
#include "blob_file_set.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/statistics.h"
#include "rocksdb/threadpool.h"
#include "table_factory.h"
#include "titan/db.h"
#include "titan_stats.h"
#include "util/repeatable_thread.h"

namespace rocksdb
{
namespace titandb
{
class PageHouseTitanCompactionFilter;

class PageHouseTitanDBImpl : public TitanDB
{
public:
    PageHouseTitanDBImpl(const TitanDBOptions & options, const std::string & dbname);

    ~PageHouseTitanDBImpl();

    Status Open(const std::vector<TitanCFDescriptor> & descs, std::vector<ColumnFamilyHandle *> * handles);

    Status Close() override;

    using TitanDB::CreateColumnFamilies;
    Status CreateColumnFamilies(const std::vector<TitanCFDescriptor> & descs, std::vector<ColumnFamilyHandle *> * handles) override;

    Status DropColumnFamilies(const std::vector<ColumnFamilyHandle *> & handles) override;

    Status DestroyColumnFamilyHandle(ColumnFamilyHandle * column_family) override;

    using TitanDB::CompactFiles;
    Status CompactFiles(const CompactionOptions & compact_options,
        ColumnFamilyHandle * column_family,
        const std::vector<std::string> & input_file_names,
        const int output_level,
        const int output_path_id = -1,
        std::vector<std::string> * const output_file_names = nullptr,
        CompactionJobInfo * compaction_job_info = nullptr) override;

    Status CloseImpl();

    using TitanDB::Put;
    Status Put(const WriteOptions & options, ColumnFamilyHandle * column_family, const Slice & key, const Slice & value) override;

    using TitanDB::Write;
    Status Write(const WriteOptions & options, WriteBatch * updates, PostWriteCallback * callback) override;

    using TitanDB::MultiBatchWrite;
    Status MultiBatchWrite(const WriteOptions & options, std::vector<WriteBatch *> && updates, PostWriteCallback * callback) override;

    using TitanDB::Delete;
    Status Delete(const WriteOptions & options, ColumnFamilyHandle * column_family, const Slice & key) override;

    using TitanDB::IngestExternalFile;
    Status IngestExternalFile(ColumnFamilyHandle * column_family,
        const std::vector<std::string> & external_files,
        const IngestExternalFileOptions & options) override;

    using TitanDB::CompactRange;
    Status CompactRange(
        const CompactRangeOptions & options, ColumnFamilyHandle * column_family, const Slice * begin, const Slice * end) override;

    using TitanDB::Flush;
    Status Flush(const FlushOptions & fopts, ColumnFamilyHandle * column_family) override;

    using TitanDB::Get;
    Status Get(const ReadOptions & options, ColumnFamilyHandle * handle, const Slice & key, PinnableSlice * value) override;

    using TitanDB::MultiGet;
    std::vector<Status> MultiGet(const ReadOptions & options,
        const std::vector<ColumnFamilyHandle *> & handles,
        const std::vector<Slice> & keys,
        std::vector<std::string> * values) override;

    using TitanDB::NewIterator;
    Iterator * NewIterator(const TitanReadOptions & options, ColumnFamilyHandle * handle) override;

    using TitanDB::NewIterators;
    Status NewIterators(
        const TitanReadOptions & options, const std::vector<ColumnFamilyHandle *> & handles, std::vector<Iterator *> * iterators) override;

    const Snapshot * GetSnapshot() override;

    void ReleaseSnapshot(const Snapshot * snapshot) override;

    using TitanDB::DisableFileDeletions;
    Status DisableFileDeletions() override;

    using TitanDB::EnableFileDeletions;
    Status EnableFileDeletions(bool force) override;

    using TitanDB::GetAllTitanFiles;
    Status GetAllTitanFiles(std::vector<std::string> & files, std::vector<VersionEdit> * edits) override;

    Status DeleteFilesInRanges(ColumnFamilyHandle * column_family, const RangePtr * ranges, size_t n, bool include_end = true) override;

    Status DeleteBlobFilesInRanges(ColumnFamilyHandle * column_family, const RangePtr * ranges, size_t n, bool include_end = true) override;

    using TitanDB::GetOptions;
    Options GetOptions(ColumnFamilyHandle * column_family) const override;

    using TitanDB::SetOptions;
    Status SetOptions(ColumnFamilyHandle * column_family, const std::unordered_map<std::string, std::string> & new_options) override;

    using TitanDB::GetTitanOptions;
    TitanOptions GetTitanOptions(ColumnFamilyHandle * column_family) const override;

    using TitanDB::GetTitanDBOptions;
    TitanDBOptions GetTitanDBOptions() const override;

    using TitanDB::GetProperty;
    bool GetProperty(ColumnFamilyHandle * column_family, const Slice & property, std::string * value) override;

    PageStoragePtr getPageStore() { return page_manager->getStore(); }

private:
    void initPageHouseLogger();
    Status OpenImpl(const std::vector<TitanCFDescriptor> & descs, std::vector<ColumnFamilyHandle *> * handles);

private:
    friend class PageHouseTitanCompactionFilter;

    std::string dbname_;
    Env * env_;
    EnvOptions env_options_;
    DBImpl * db_impl_;
    TitanDBOptions db_options_;

    Poco::AutoPtr<Poco::FileChannel> log_file;

    PageHouseManagerPtr page_manager;
};

} // namespace titandb
} // namespace rocksdb
