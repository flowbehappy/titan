#include <cinttypes>
#include <unordered_map>

#include "PageHouse_db_impl.h"
#include "blob_file_iterator.h"
#include "blob_file_reader.h"
#include "blob_file_size_collector.h"
#include "db/db_impl/db_impl.h"
#include "db_iter.h"
#include "file/filename.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/utilities/debug.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "titan/db.h"
#include "titan_fault_injection_test_env.h"
#include "util/random.h"

namespace rocksdb
{
namespace titandb
{

void DeleteDir(Env * env, const std::string & dirname)
{
    Poco::File file(dirname);
    if (file.exists())
        file.remove(true);
}

void CreateDir(const std::string & dirname)
{
    Poco::File file(dirname);
    if (!file.exists())
        file.createDirectory();
}

class PageHouseTitanDBTest : public testing::Test
{
public:
    PageHouseTitanDBTest() : dbname_(test::TmpDir())
    {
        options_.dirname = dbname_ + "/pagehouse";
        options_.create_if_missing = true;
        options_.min_blob_size = 32;
        options_.min_gc_batch_size = 1;
        options_.merge_small_file_threshold = 0;
        options_.disable_background_gc = true;
        options_.blob_file_compression = CompressionType::kLZ4Compression;
        options_.statistics = CreateDBStatistics();
        DeleteDir(env_, options_.dirname);
        DeleteDir(env_, dbname_);
        CreateDir(dbname_);
    }

    ~PageHouseTitanDBTest()
    {
        Close();
        DeleteDir(env_, options_.dirname);
        DeleteDir(env_, dbname_);
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
    }

    void Open()
    {
        if (cf_names_.empty())
        {
            ASSERT_OK(TitanDB::Open(options_, dbname_, &db_, true));
            db_impl_ = dynamic_cast<PageHouseTitanDBImpl *>(db_);
        }
        else
        {
            TitanDBOptions db_options(options_);
            TitanCFOptions cf_options(options_);
            cf_names_.clear();
            ASSERT_OK(DB::ListColumnFamilies(db_options, dbname_, &cf_names_));
            std::vector<TitanCFDescriptor> descs;
            for (auto & name : cf_names_)
            {
                descs.emplace_back(name, cf_options);
            }
            cf_handles_.clear();
            ASSERT_OK(TitanDB::Open(db_options, dbname_, descs, &cf_handles_, &db_, true));
            db_impl_ = dynamic_cast<PageHouseTitanDBImpl *>(db_);
        }
    }

    void Close()
    {
        if (!db_)
            return;
        for (auto & handle : cf_handles_)
        {
            db_->DestroyColumnFamilyHandle(handle);
        }
        ASSERT_OK(db_->Close());
        delete db_;
        db_ = nullptr;
    }

    void Reopen()
    {
        Close();
        Open();
    }

    void AddCF(const std::string & name)
    {
        TitanCFDescriptor desc(name, options_);
        ColumnFamilyHandle * handle = nullptr;
        ASSERT_OK(db_->CreateColumnFamily(desc, &handle));
        cf_names_.emplace_back(name);
        cf_handles_.emplace_back(handle);
    }

    void DropCF(const std::string & name)
    {
        for (size_t i = 0; i < cf_names_.size(); i++)
        {
            if (cf_names_[i] != name)
                continue;
            auto handle = cf_handles_[i];
            ASSERT_OK(db_->DropColumnFamily(handle));
            db_->DestroyColumnFamilyHandle(handle);
            cf_names_.erase(cf_names_.begin() + i);
            cf_handles_.erase(cf_handles_.begin() + i);
            break;
        }
    }

    void Put(uint64_t k, std::map<std::string, std::string> * data = nullptr)
    {
        WriteOptions wopts;
        std::string key = GenKey(k);
        std::string value = GenValue(k);
        ASSERT_OK(db_->Put(wopts, key, value));
        for (auto & handle : cf_handles_)
        {
            ASSERT_OK(db_->Put(wopts, handle, key, value));
        }
        if (data != nullptr)
        {
            data->emplace(key, value);
        }
    }

    void Delete(uint64_t k)
    {
        WriteOptions wopts;
        std::string key = GenKey(k);
        ASSERT_OK(db_->Delete(wopts, key));
        for (auto & handle : cf_handles_)
        {
            ASSERT_OK(db_->Delete(wopts, handle, key));
        }
    }

    void Flush(ColumnFamilyHandle * cf_handle = nullptr)
    {
        if (cf_handle == nullptr)
        {
            cf_handle = db_->DefaultColumnFamily();
        }
        FlushOptions fopts;
        ASSERT_OK(db_->Flush(fopts));
        for (auto & handle : cf_handles_)
        {
            ASSERT_OK(db_->Flush(fopts, handle));
        }
    }

    bool GetIntProperty(const Slice & property, uint64_t * value) { return db_->GetIntProperty(property, value); }

    ColumnFamilyHandle * GetColumnFamilyHandle(uint32_t cf_id)
    {
        return db_impl_->getBaseDB()->GetColumnFamilyHandleUnlocked(cf_id).release();
    }

    void VerifyDB(const std::map<std::string, std::string> & data, ReadOptions ropts = ReadOptions())
    {
        for (auto & kv : data)
        {
            std::string value;
            ASSERT_OK(db_->Get(ropts, kv.first, &value));
            ASSERT_EQ(value, kv.second);
            for (auto & handle : cf_handles_)
            {
                ASSERT_OK(db_->Get(ropts, handle, kv.first, &value));
                ASSERT_EQ(value, kv.second);
            }
            std::vector<Slice> keys(cf_handles_.size(), kv.first);
            std::vector<std::string> values;
            auto res = db_->MultiGet(ropts, cf_handles_, keys, &values);
            for (auto & s : res)
                ASSERT_OK(s);
            for (auto & v : values)
                ASSERT_EQ(v, kv.second);
        }

        std::vector<Iterator *> iterators;
        db_->NewIterators(ropts, cf_handles_, &iterators);
        iterators.emplace_back(db_->NewIterator(ropts));
        for (auto & handle : cf_handles_)
        {
            iterators.emplace_back(db_->NewIterator(ropts, handle));
        }
        for (auto & iter : iterators)
        {
            iter->SeekToFirst();
            for (auto & kv : data)
            {
                ASSERT_EQ(iter->Valid(), true);
                ASSERT_EQ(iter->key(), kv.first);
                ASSERT_EQ(iter->value(), kv.second);
                iter->Next();
            }
            delete iter;
        }
    }

    // Note:
    // - When level is bottommost, always compact, no trivial move.
    void CompactAll(ColumnFamilyHandle * cf_handle = nullptr, int level = -1)
    {
        if (cf_handle == nullptr)
        {
            cf_handle = db_->DefaultColumnFamily();
        }
        auto opts = db_->GetOptions();
        if (level < 0)
        {
            level = opts.num_levels - 1;
        }
        auto compact_opts = CompactRangeOptions();
        compact_opts.change_level = true;
        compact_opts.target_level = level;
        if (level >= opts.num_levels - 1)
        {
            compact_opts.bottommost_level_compaction = BottommostLevelCompaction::kForce;
        }
        ASSERT_OK(db_->CompactRange(compact_opts, cf_handle, nullptr, nullptr));
    }

    void DeleteFilesInRange(const Slice * begin, const Slice * end)
    {
        RangePtr range(begin, end);
        ASSERT_OK(db_->DeleteFilesInRanges(db_->DefaultColumnFamily(), &range, 1));
        ASSERT_OK(db_->DeleteBlobFilesInRanges(db_->DefaultColumnFamily(), &range, 1));
    }

    std::string GenKey(uint64_t i)
    {
        char buf[64];
        snprintf(buf, sizeof(buf), "k-%08" PRIu64, i);
        return buf;
    }

    std::string GenValue(uint64_t k)
    {
        if (k % 2 == 0)
        {
            return std::string(options_.min_blob_size - 1, 'v');
        }
        else
        {
            return std::string(options_.min_blob_size + 1, 'v');
        }
    }

    void TestTableFactory()
    {
        DeleteDir(env_, options_.dirname);
        DeleteDir(env_, dbname_);
        Options options;
        options.create_if_missing = true;
        options.table_factory.reset(NewBlockBasedTableFactory(BlockBasedTableOptions()));
        auto * original_table_factory = options.table_factory.get();
        TitanDB * db;
        ASSERT_OK(TitanDB::Open(TitanOptions(options), dbname_, &db, true));
        auto cf_options = db->GetOptions(db->DefaultColumnFamily());
        auto db_options = db->GetDBOptions();
        ImmutableCFOptions immu_cf_options(cf_options);
        ASSERT_EQ(original_table_factory, immu_cf_options.table_factory.get());
        ASSERT_OK(db->Close());
        delete db;

        DeleteDir(env_, options_.dirname);
        DeleteDir(env_, dbname_);
    }

    // Make db ignore first bg_error
    class BGErrorListener : public EventListener
    {
    public:
        void OnBackgroundError(BackgroundErrorReason reason, Status * error) override
        {
            if (++cnt == 1)
                *error = Status();
        }

    private:
        int cnt{0};
    };

    Env * env_{Env::Default()};
    std::string dbname_;
    TitanOptions options_;
    TitanDB * db_{nullptr};
    PageHouseTitanDBImpl * db_impl_{nullptr};
    std::vector<std::string> cf_names_;
    std::vector<ColumnFamilyHandle *> cf_handles_;
};

TEST_F(PageHouseTitanDBTest, Basic)
{
    const uint64_t kNumKeys = 100;
    std::map<std::string, std::string> data;
    for (auto i = 0; i < 6; i++)
    {
        if (i == 0)
        {
            Open();
        }
        else
        {
            Reopen();
            VerifyDB(data);
            AddCF(std::to_string(i));
            if (i % 3 == 0)
            {
                DropCF(std::to_string(i - 1));
                DropCF(std::to_string(i - 2));
            }
        }
        for (uint64_t k = 1; k <= kNumKeys; k++)
        {
            Put(k, &data);
        }
        Flush();
        VerifyDB(data);
    }
}

TEST_F(PageHouseTitanDBTest, DictCompressOptions)
{
#if ZSTD_VERSION_NUMBER >= 10103
    options_.min_blob_size = 1;
    options_.blob_file_compression = CompressionType::kZSTD;
    options_.blob_file_compression_options.window_bits = -14;
    options_.blob_file_compression_options.level = 32767;
    options_.blob_file_compression_options.strategy = 0;
    options_.blob_file_compression_options.max_dict_bytes = 6400;
    options_.blob_file_compression_options.zstd_max_train_bytes = 0;

    const uint64_t kNumKeys = 500;
    std::map<std::string, std::string> data;
    Open();
    for (uint64_t k = 1; k <= kNumKeys; k++)
    {
        Put(k, &data);
    }
    Flush();
    VerifyDB(data);
#endif
}

TEST_F(PageHouseTitanDBTest, TableFactory)
{
    TestTableFactory();
}

TEST_F(PageHouseTitanDBTest, DbIter)
{
    Open();
    std::map<std::string, std::string> data;
    const int kNumEntries = 100;
    for (uint64_t i = 1; i <= kNumEntries; i++)
    {
        Put(i, &data);
    }
    Flush();
    ASSERT_EQ(kNumEntries, data.size());
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToFirst();
    for (const auto & it : data)
    {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(it.first, iter->key());
        ASSERT_EQ(it.second, iter->value());
        iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
}

TEST_F(PageHouseTitanDBTest, DBIterSeek)
{
    Open();
    std::map<std::string, std::string> data;
    const int kNumEntries = 100;
    for (uint64_t i = 1; i <= kNumEntries; i++)
    {
        Put(i, &data);
    }
    Flush();
    ASSERT_EQ(kNumEntries, data.size());
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(data.begin()->first, iter->key());
    ASSERT_EQ(data.begin()->second, iter->value());
    iter->SeekToLast();
    ASSERT_EQ(data.rbegin()->first, iter->key());
    ASSERT_EQ(data.rbegin()->second, iter->value());
    for (auto it = data.rbegin(); it != data.rend(); it++)
    {
        iter->SeekToLast();
        ASSERT_TRUE(iter->Valid());
        iter->SeekForPrev(it->first);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(it->first, iter->key());
        ASSERT_EQ(it->second, iter->value());
    }
    for (const auto & it : data)
    {
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        iter->Seek(it.first);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(it.first, iter->key());
        ASSERT_EQ(it.second, iter->value());
    }
}

TEST_F(PageHouseTitanDBTest, Snapshot)
{
    Open();
    std::map<std::string, std::string> data;
    Put(1, &data);
    ASSERT_EQ(1, data.size());

    const Snapshot * snapshot(db_->GetSnapshot());
    ReadOptions ropts;
    ropts.snapshot = snapshot;

    VerifyDB(data, ropts);
    Flush();
    VerifyDB(data, ropts);
    db_->ReleaseSnapshot(snapshot);
}

TEST_F(PageHouseTitanDBTest, IngestExternalFiles)
{
    Open();
    SstFileWriter sst_file_writer(EnvOptions(), options_);
    ASSERT_EQ(sst_file_writer.FileSize(), 0);

    const uint64_t kNumEntries = 100;
    std::map<std::string, std::string> total_data;
    std::map<std::string, std::string> original_data;
    std::map<std::string, std::string> ingested_data;
    for (uint64_t i = 1; i <= kNumEntries; i++)
    {
        Put(i, &original_data);
    }
    ASSERT_EQ(kNumEntries, original_data.size());
    total_data.insert(original_data.begin(), original_data.end());
    VerifyDB(total_data);
    Flush();
    VerifyDB(total_data);

    const uint64_t kNumIngestedEntries = 100;
    // Make sure that keys in SST overlaps with existing keys
    const uint64_t kIngestedStart = kNumEntries - kNumEntries / 2;
    std::string sst_file = options_.dirname + "/for_ingest.sst";
    ASSERT_OK(sst_file_writer.Open(sst_file));
    for (uint64_t i = 1; i <= kNumIngestedEntries; i++)
    {
        std::string key = GenKey(kIngestedStart + i);
        std::string value = GenValue(kIngestedStart + i);
        ASSERT_OK(sst_file_writer.Put(key, value));
        total_data[key] = value;
        ingested_data.emplace(key, value);
    }
    ASSERT_OK(sst_file_writer.Finish());
    IngestExternalFileOptions ifo;
    ASSERT_OK(db_->IngestExternalFile({sst_file}, ifo));
    VerifyDB(total_data);
    Flush();
    VerifyDB(total_data);
    //    for (auto & handle : cf_handles_)
    //    {
    //        auto blob = GetBlobStorage(handle);
    //        ASSERT_EQ(1, blob.lock()->NumBlobFiles());
    //    }
    //
    //    CompactRangeOptions copt;
    //    ASSERT_OK(db_->CompactRange(copt, nullptr, nullptr));
    //    VerifyDB(total_data);
    //    for (auto & handle : cf_handles_)
    //    {
    //        auto blob = GetBlobStorage(handle);
    //        ASSERT_EQ(2, blob.lock()->NumBlobFiles());
    //        std::map<uint64_t, std::weak_ptr<BlobFileMeta>> blob_files;
    //        blob.lock()->ExportBlobFiles(blob_files);
    //        ASSERT_EQ(2, blob_files.size());
    //        auto bf = blob_files.begin();
    //        VerifyBlob(bf->first, original_data);
    //        bf++;
    //        VerifyBlob(bf->first, ingested_data);
    //    }
}

//TEST_F(PageHouseTitanDBTest, NewColumnFamilyHasBlobFileSizeCollector)
//{
//    Open();
//    AddCF("new_cf");
//    Options opt = db_->GetOptions(cf_handles_.back());
//    ASSERT_EQ(1, opt.table_properties_collector_factories.size());
//    std::unique_ptr<BlobFileSizeCollectorFactory> prop_collector_factory(new BlobFileSizeCollectorFactory());
//    ASSERT_EQ(std::string(prop_collector_factory->Name()), std::string(opt.table_properties_collector_factories[0]->Name()));
//}

TEST_F(PageHouseTitanDBTest, DropColumnFamily)
{
    Open();
    const uint64_t kNumCF = 3;
    for (uint64_t i = 1; i <= kNumCF; i++)
    {
        AddCF(std::to_string(i));
    }
    const uint64_t kNumEntries = 100;
    std::map<std::string, std::string> data;
    for (uint64_t i = 1; i <= kNumEntries; i++)
    {
        Put(i, &data);
    }
    VerifyDB(data);
    Flush();
    VerifyDB(data);

    // Destroy column families handle, check whether the data is preserved after a
    // round of GC and restart.
    for (auto & handle : cf_handles_)
    {
        db_->DestroyColumnFamilyHandle(handle);
    }
    cf_handles_.clear();
    VerifyDB(data);
    Reopen();
    VerifyDB(data);

    for (auto & handle : cf_handles_)
    {
        // we can't drop default column family
        if (handle->GetName() == kDefaultColumnFamilyName)
        {
            continue;
        }
        ASSERT_OK(db_->DropColumnFamily(handle));
        // The data is actually deleted only after destroying all outstanding column
        // family handles, so we can still read from the dropped column family.
        VerifyDB(data);
    }

    Close();
}

TEST_F(PageHouseTitanDBTest, DestroyColumnFamilyHandle)
{
    Open();
    const uint64_t kNumCF = 3;
    for (uint64_t i = 1; i <= kNumCF; i++)
    {
        AddCF(std::to_string(i));
    }
    const uint64_t kNumEntries = 10;
    std::map<std::string, std::string> data;
    for (uint64_t i = 1; i <= kNumEntries; i++)
    {
        Put(i, &data);
    }
    VerifyDB(data);
    Flush();
    VerifyDB(data);

    // Destroy column families handle, check whether GC skips the column families.
    for (auto & handle : cf_handles_)
    {
        auto cf_id = handle->GetID();
        db_->DestroyColumnFamilyHandle(handle);
        //        ASSERT_OK(db_impl_->TEST_StartGC(cf_id));
    }
    cf_handles_.clear();
    VerifyDB(data);

    Reopen();
    VerifyDB(data);
    Close();
}

TEST_F(PageHouseTitanDBTest, DeleteFilesInRange)
{
    Open();

    ASSERT_OK(db_->Put(WriteOptions(), GenKey(11), GenValue(1)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(21), GenValue(2)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(31), GenValue(3)));
    Flush();
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(41), GenValue(4)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(51), GenValue(5)));
    Flush();
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(61), GenValue(6)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(71), GenValue(7)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(81), GenValue(8)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(91), GenValue(9)));
    Flush();
    // Not bottommost, we need trivial move.
    CompactAll(nullptr, 5);

    std::string value;
    ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &value));
    ASSERT_EQ(value, "0");
    ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level5", &value));
    ASSERT_EQ(value, "3");

    ASSERT_OK(db_->Put(WriteOptions(), GenKey(12), GenValue(1)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(22), GenValue(2)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(32), GenValue(3)));
    Flush();
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(42), GenValue(4)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(52), GenValue(5)));
    Flush();
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(62), GenValue(6)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(72), GenValue(7)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(82), GenValue(8)));
    ASSERT_OK(db_->Put(WriteOptions(), GenKey(92), GenValue(9)));
    Flush();

    // The LSM structure is:
    // L0: [11, 21, 31] [41, 51] [61, 71, 81, 91]
    // L5: [12, 22, 32] [42, 52] [62, 72, 82, 92]
    // with 6 alive blob files
    ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &value));
    ASSERT_EQ(value, "3");
    ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level5", &value));
    ASSERT_EQ(value, "3");

    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());

    std::string key40 = GenKey(40);
    std::string key80 = GenKey(80);
    Slice start = Slice(key40);
    Slice end = Slice(key80);
    DeleteFilesInRange(&start, &end);

    // Now the LSM structure is:
    // L0: [11, 21, 31] [41, 51] [61, 71, 81, 91]
    // L5: [12, 22, 32]          [62, 72, 82, 92]
    // with 4 alive blob files and 2 obsolete blob files
    ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &value));
    ASSERT_EQ(value, "3");
    ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level5", &value));
    ASSERT_EQ(value, "2");

    //    auto blob = GetBlobStorage(db_->DefaultColumnFamily()).lock();
    //    ASSERT_EQ(blob->NumBlobFiles(), 6);
    //    // These two files are marked obsolete directly by `DeleteBlobFilesInRanges`
    //    ASSERT_EQ(blob->NumObsoleteBlobFiles(), 2);
    //
    //    // The snapshot held by the iterator prevents the blob files from being
    //    // purged.
    //    ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
    while (iter->Valid())
    {
        iter->Next();
        ASSERT_OK(iter->status());
    }
    //    ASSERT_EQ(blob->NumBlobFiles(), 6);
    //    ASSERT_EQ(blob->NumObsoleteBlobFiles(), 2);
    //
    // Once the snapshot is released, the blob files should be purged.
    iter.reset(nullptr);
    //    ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
    //    ASSERT_EQ(blob->NumBlobFiles(), 4);
    //    ASSERT_EQ(blob->NumObsoleteBlobFiles(), 0);

    Close();
}

TEST_F(PageHouseTitanDBTest, SetOptions)
{
    options_.write_buffer_size = 42000000;
    options_.min_blob_size = 123;
    options_.blob_run_mode = TitanBlobRunMode::kReadOnly;
    Open();

    TitanOptions titan_options = db_->GetTitanOptions();
    ASSERT_EQ(42000000, titan_options.write_buffer_size);
    ASSERT_EQ(123, titan_options.min_blob_size);
    ASSERT_EQ(TitanBlobRunMode::kReadOnly, titan_options.blob_run_mode);

    std::unordered_map<std::string, std::string> opts;

    // Set titan options.
    opts["blob_run_mode"] = "kReadOnly";
    ASSERT_OK(db_->SetOptions(opts));
    titan_options = db_->GetTitanOptions();
    ASSERT_EQ(TitanBlobRunMode::kReadOnly, titan_options.blob_run_mode);
    opts.clear();

    // Set column family options.
    opts["disable_auto_compactions"] = "true";
    ASSERT_OK(db_->SetOptions(opts));
    titan_options = db_->GetTitanOptions();
    ASSERT_TRUE(titan_options.disable_auto_compactions);
    opts.clear();

    // Set DB options.
    opts["max_background_jobs"] = "15";
    ASSERT_OK(db_->SetDBOptions(opts));
    titan_options = db_->GetTitanOptions();
    ASSERT_EQ(15, titan_options.max_background_jobs);
    TitanDBOptions titan_db_options = db_->GetTitanDBOptions();
    ASSERT_EQ(15, titan_db_options.max_background_jobs);
}

TEST_F(PageHouseTitanDBTest, VeryLargeValue)
{
    Open();

    ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100 * 1024 * 1024, 'v')));
    Flush();

    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
    ASSERT_EQ(value.size(), 100 * 1024 * 1024);

    Close();
}

TEST_F(PageHouseTitanDBTest, UpdateValue)
{
    options_.min_blob_size = 1024;
    Open();

    ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
    ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10, 'v')));
    Flush();
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
    ASSERT_EQ(value.size(), 10);
    ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100, 'v')));
    Flush();
    ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
    ASSERT_EQ(value.size(), 100);
    CompactAll();
    ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
    ASSERT_EQ(value.size(), 100);

    Close();
}

TEST_F(PageHouseTitanDBTest, MultiGet)
{
    options_.min_blob_size = 1024;
    std::vector<int> blob_cache_sizes = {0, 15 * 1024};
    std::vector<int> block_cache_sizes = {0, 150};

    for (auto blob_cache_size : blob_cache_sizes)
    {
        for (auto block_cache_size : block_cache_sizes)
        {
            BlockBasedTableOptions table_opts;
            table_opts.block_size = block_cache_size;
            options_.table_factory.reset(NewBlockBasedTableFactory(table_opts));
            options_.blob_cache = NewLRUCache(blob_cache_size);

            Open();
            ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100, 'v')));
            ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(100, 'v')));
            ASSERT_OK(db_->Put(WriteOptions(), "k3", std::string(10 * 1024, 'v')));
            ASSERT_OK(db_->Put(WriteOptions(), "k4", std::string(100 * 1024, 'v')));
            Flush();

            std::vector<std::string> values;
            db_->MultiGet(ReadOptions(), std::vector<Slice>{"k1", "k2", "k3", "k4"}, &values);
            ASSERT_EQ(values[0].size(), 100);
            ASSERT_EQ(values[1].size(), 100);
            ASSERT_EQ(values[2].size(), 10 * 1024);
            ASSERT_EQ(values[3].size(), 100 * 1024);
            Close();
            DeleteDir(env_, options_.dirname);
            DeleteDir(env_, dbname_);
        }
    }
}

TEST_F(PageHouseTitanDBTest, PrefixScan)
{
    options_.min_blob_size = 1024;
    options_.prefix_extractor.reset(NewFixedPrefixTransform(3));
    std::vector<int> blob_cache_sizes = {0, 15 * 1024};
    std::vector<int> block_cache_sizes = {0, 150};

    for (auto blob_cache_size : blob_cache_sizes)
    {
        for (auto block_cache_size : block_cache_sizes)
        {
            BlockBasedTableOptions table_opts;
            table_opts.block_size = block_cache_size;
            options_.table_factory.reset(NewBlockBasedTableFactory(table_opts));
            options_.blob_cache = NewLRUCache(blob_cache_size);

            Open();
            ASSERT_OK(db_->Put(WriteOptions(), "abc1", std::string(100, 'v')));
            ASSERT_OK(db_->Put(WriteOptions(), "abc2", std::string(2 * 1024, 'v')));
            ASSERT_OK(db_->Put(WriteOptions(), "cba1", std::string(100, 'v')));
            ASSERT_OK(db_->Put(WriteOptions(), "cba1", std::string(10 * 1024, 'v')));
            Flush();

            ReadOptions r_opt;
            r_opt.prefix_same_as_start = true;
            {
                std::unique_ptr<Iterator> iter(db_->NewIterator(r_opt));
                iter->Seek("abc");
                ASSERT_TRUE(iter->Valid());
                ASSERT_EQ("abc1", iter->key());
                ASSERT_EQ(iter->value().size(), 100);
                iter->Next();

                ASSERT_TRUE(iter->Valid());
                ASSERT_EQ("abc2", iter->key());
                ASSERT_EQ(iter->value().size(), 2 * 1024);
                iter->Next();

                ASSERT_FALSE(iter->Valid());
                ;
            }
            Close();
            DeleteDir(env_, options_.dirname);
            DeleteDir(env_, dbname_);
        }
    }
}

TEST_F(PageHouseTitanDBTest, CompressionTypes)
{
    options_.min_blob_size = 1024;
    auto compressions = std::vector<CompressionType>{
        CompressionType::kNoCompression, CompressionType::kLZ4Compression, CompressionType::kSnappyCompression, CompressionType::kZSTD};

    for (auto type : compressions)
    {
        options_.blob_file_compression = type;
        Open();
        ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
        Flush();
        Close();
    }
}

TEST_F(PageHouseTitanDBTest, DifferentCompressionType)
{
    options_.min_blob_size = 1024;
    options_.blob_file_compression = CompressionType::kSnappyCompression;

    Open();
    ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
    Flush();
    Close();

    options_.blob_file_compression = CompressionType::kLZ4Compression;
    Open();
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
    ASSERT_EQ(value, std::string(10 * 1024, 'v'));
    Close();
}

TEST_F(PageHouseTitanDBTest, EmptyCompaction)
{
    Open();
    CompactAll();
    Close();
}

TEST_F(PageHouseTitanDBTest, SmallCompaction)
{
    Open();
    ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10, 'v')));
    ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(10, 'v')));
    Flush();
    CompactAll();
    Close();
}

TEST_F(PageHouseTitanDBTest, MixCompaction)
{
    Open();
    ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(0, 'v')));
    ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(100, 'v')));
    ASSERT_OK(db_->Put(WriteOptions(), "k3", std::string(10 * 1024, 'v')));
    Flush();
    CompactAll();
    Close();
}

TEST_F(PageHouseTitanDBTest, LargeCompaction)
{
    Open();
    ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(10 * 1024, 'v')));
    ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(10 * 1024, 'v')));
    Flush();
    CompactAll();
    Close();
}

TEST_F(PageHouseTitanDBTest, Config)
{
    options_.disable_background_gc = false;

    options_.max_background_gc = 0;
    Open();
    options_.max_background_gc = 1;
    Reopen();
    options_.max_background_gc = 2;
    Reopen();

    options_.min_blob_size = 512;
    Reopen();
    options_.min_blob_size = 1024;
    Reopen();
    options_.min_blob_size = 64 * 1024 * 1024;
    Reopen();

    options_.blob_file_discardable_ratio = 0;
    Reopen();
    options_.blob_file_discardable_ratio = 0.001;
    Reopen();
    options_.blob_file_discardable_ratio = 1;

    options_.blob_cache = NewLRUCache(0);
    Reopen();
    options_.blob_cache = NewLRUCache(1 * 1024 * 1024);
    Reopen();

    Close();
}

#if defined(__linux) && !defined(TRAVIS)
TEST_F(PageHouseTitanDBTest, DISABLED_NoSpaceLeft)
{
    options_.disable_background_gc = false;
    system(("mkdir -p " + dbname_).c_str());
    system(("sudo mount -t tmpfs -o size=1m tmpfs " + dbname_).c_str());
    Open();

    ASSERT_OK(db_->Put(WriteOptions(), "k1", std::string(100 * 1024, 'v')));
    ASSERT_OK(db_->Put(WriteOptions(), "k2", std::string(100 * 1024, 'v')));
    ASSERT_OK(db_->Put(WriteOptions(), "k3", std::string(100 * 1024, 'v')));
    Flush();
    ASSERT_OK(db_->Put(WriteOptions(), "k4", std::string(300 * 1024, 'v')));
    ASSERT_NOK(db_->Flush(FlushOptions()));

    Close();
    system(("sudo umount -l " + dbname_).c_str());
}
#endif
} // namespace titandb
} // namespace rocksdb

int main(int argc, char ** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
