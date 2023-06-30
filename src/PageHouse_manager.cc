#include "PageHouse_manger.h"

namespace rocksdb
{
namespace titandb
{
using namespace DB;

PageHouseManager::PageHouseManager(const String & root_dir_) : log(&Poco::Logger::get("PageHouseManager")), root_dir(root_dir_)
{
    auto file_provider = std::make_shared<FileProvider>();
    PathPool path_pool({root_dir}, file_provider);
    PSDiskDelegatorPtr delegator = std::make_shared<PSDiskDelegatorGlobalSingle>(path_pool, "v1");
    PageStorageConfig config;
    pagestore = PageStorage::create("pagehouse_light", delegator, config, file_provider);
    pagestore->restore();
    max_pageid = pagestore->getMaxId();

    auto dp = delegator->defaultPath();

    LOG_DEBUG(log, "delegator->defaultPath(): {}", dp);
}

PageHouseManager::~PageHouseManager()
{
}

} // namespace titandb
} // namespace rocksdb