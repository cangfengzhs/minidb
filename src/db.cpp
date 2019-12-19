//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_DP_CPP
#define MINIDB_DP_CPP

#include "db.h"

#include <utility>
#include "slice.h"
#include "file_util.h"
#include "format.h"
#include "db_impl.h"
namespace minidb{
    DB DB::open(const std::string &db_name) {
        DB db;
        db.impl = DBImpl::open(db_name);
        return std::move(db);
    }
    DB DB::create(const std::string &db_name) {
        DB db;
        db.impl = DBImpl::create(db_name);
        return std::move(db);
    }
    DB DB::open_or_create(const std::string &db_name) {
        DB db;
        db.impl = DBImpl::open(db_name);
        if(!db.impl){
            db.impl = DBImpl::create(db_name);
        }
        return db;
    }
    std::shared_ptr<Slice> DB::get(std::shared_ptr<Slice> key) {
        return impl->get(key);
    }
    void DB::set(std::shared_ptr<Slice> key, std::shared_ptr<Slice> value) {
        return impl->set(key,value);
    }
    void DB::remove(std::shared_ptr<Slice> key) {
        return impl->remove(key);
    }
}
#endif //MINIDB_DP_CPP
