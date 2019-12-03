//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_DB_H
#define MINIDB_DB_H

#include "slice.h"
#include <memory>
namespace minidb {
    class DBImpl;
    class DB {
        std::shared_ptr<DBImpl> impl;
        DB()= default;
    public:
        static DB create(const std::string& db_name);
        static DB open(const std::string& db_name);
        static DB open_or_create(const std::string& db_name);
        void set(std::shared_ptr<Slice> key,std::shared_ptr<Slice> value);
        std::shared_ptr<Slice> get(std::shared_ptr<Slice> key);
        void remove(std::shared_ptr<Slice> key);
    };
}


#endif //MINIDB_DB_H
