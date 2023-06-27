#include "PageHouse_manger.h"

namespace rocksdb
{
namespace titandb
{
using namespace DB;

PageHouseManager::PageHouseManager(const String & dbname) : root_dir(dbname + "/pagehouse"), thread_pool(1, "PH_BG_")
{
    auto file_provider = std::make_shared<FileProvider>();
    PathPool path_pool({dbname}, file_provider);
    PSDiskDelegatorPtr delegator = std::make_shared<PSDiskDelegatorGlobalSingle>(path_pool, "pagehouse");
    PageStorageConfig config;
    pagestore = PageStorage::create("universal_pagehouse", delegator, config, file_provider);
    pagestore->restore();
    max_pageid = pagestore->getMaxId();
    gc_handle = thread_pool.addTask([this]() { return pagestore->gc(); });
}

PageHouseManager::~PageHouseManager()
{
    if (!gc_handle)
        return;
    thread_pool.removeTask(gc_handle);
    gc_handle = nullptr;
}

} // namespace titandb
} // namespace rocksdb