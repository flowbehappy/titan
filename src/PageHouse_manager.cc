#include "PageHouse_manger.h"

namespace rocksdb
{
namespace titandb
{
using namespace DB;

PageHouseManager::PageHouseManager(const String & root_dir_)
    : log(&Poco::Logger::get("PageHouseManager")), root_dir(root_dir_), path_pool({root_dir}, std::make_shared<FileProvider>())
{
    PSDiskDelegatorPtr delegator = path_pool.getPSDiskDelegatorGlobalSingle("v1");
    PageStorageConfig config;
    pagestore = PageStorage::create("pagehouse_light", delegator, config, path_pool.getFileProvider());
    pagestore->restore();
    max_pageid = pagestore->getMaxId();

    auto dp = delegator->defaultPath();

    LOG_DEBUG(log, "delegator->defaultPath(): {}", dp);
}

PageHouseManager::~PageHouseManager() {}

} // namespace titandb
} // namespace rocksdb