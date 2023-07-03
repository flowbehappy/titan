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
    PageHouseManager(const String & root_dir);

    ~PageHouseManager();

    PageStoragePtr getStore() { return pagestore; }
    String getRootDir() { return root_dir; }

    UInt64 getNextPageId() { return max_pageid.fetch_add(1, std::memory_order_relaxed) + 1; }
    UInt64 getMaxPageId() { return max_pageid.load(std::memory_order_relaxed); }

    const CompressionSettings & getCompressionSettings() { return compress_setting; }

    bool triggerGC() { return pagestore->gc(); }

private:
    Poco::Logger * log;

    String root_dir;
    PathPool path_pool;
    PageStoragePtr pagestore;

    CompressionSettings compress_setting{CompressionMethod::LZ4};

    std::atomic<UInt64> max_pageid;
};

using PageHouseManagerPtr = std::shared_ptr<PageHouseManager>;
} // namespace titandb
} // namespace rocksdb