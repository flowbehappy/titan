#include "titan/db.h"

#include "PageHouse_db_impl.h"
#include "db_impl.h"

namespace rocksdb
{
namespace titandb
{

Status TitanDB::Open(const TitanOptions & options, const std::string & dbname, TitanDB ** db, bool use_pagehouse)
{
    TitanDBOptions db_options(options);
    TitanCFOptions cf_options(options);
    std::vector<TitanCFDescriptor> descs;
    descs.emplace_back(kDefaultColumnFamilyName, cf_options);
    std::vector<ColumnFamilyHandle *> handles;
    Status s = TitanDB::Open(db_options, dbname, descs, &handles, db, use_pagehouse);
    if (s.ok())
    {
        assert(handles.size() == 1);
        // DBImpl is always holding the default handle.
        delete handles[0];
    }
    return s;
}

Status TitanDB::Open(const TitanDBOptions & db_options,
    const std::string & dbname,
    const std::vector<TitanCFDescriptor> & descs,
    std::vector<ColumnFamilyHandle *> * handles,
    TitanDB ** db,
    bool use_pagehouse)
{
    auto [impl, s] = [&]() -> std::pair<TitanDB *, Status> {
        if (use_pagehouse)
        {
            auto ph = new PageHouseTitanDBImpl(db_options, dbname);
            return {ph, ph->Open(descs, handles)};
        }
        else
        {
            auto titan = new TitanDBImpl(db_options, dbname);
            return {titan, titan->Open(descs, handles)};
        }
    }();

    if (s.ok())
    {
        *db = impl;
    }
    else
    {
        *db = nullptr;
        delete impl;
    }
    return s;
}

} // namespace titandb
} // namespace rocksdb
