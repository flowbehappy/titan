#pragma once

#include <Common/BackgroundProcessingPool.h>
#include <IO/CompressionSettings.h>
#include <V3/PageStorageImpl.h>
#include <V3/Universal/UniversalPageStorage.h>

#include "db/db_impl/db_impl.h"
#include "titan/options.h"
#include "util/repeatable_thread.h"

namespace rocksdb
{
namespace titandb
{
using namespace DB;

class PageHouseManager
{
public:
    PageHouseManager(const String & dbname);

    ~PageHouseManager();

    PageStoragePtr getStore() { return pagestore; }
    String getRootDir() { return root_dir; }

    UInt64 getAndIncrNextPageId() { return max_pageid.fetch_add(1, std::memory_order_relaxed); }
    UInt64 getMaxPageId() { return max_pageid.load(std::memory_order_relaxed); }

    const CompressionSettings & getCompressionSettings() { return compress_setting; }

private:
    String root_dir;
    PageStoragePtr pagestore;
    BackgroundProcessingPool thread_pool;
    BackgroundProcessingPool::TaskHandle gc_handle;

    CompressionSettings compress_setting{CompressionMethod::LZ4};

    std::atomic<UInt64> max_pageid;
};

using PageHouseManagerPtr = std::shared_ptr<PageHouseManager>;
} // namespace titandb
} // namespace rocksdb