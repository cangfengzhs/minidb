//
// Created by cangfeng on 2019/12/2.
//

#ifndef MINIDB_DB_IMPL_H
#define MINIDB_DB_IMPL_H

#include "slice.h"
#include "memtable.h"
#include "format.h"
#include "log_writer.h"
#include "sstable.h"
#include "version.h"
namespace minidb{
    class DBImpl{
        std::string db_name_;
        ptr<MemTable> memtable_;
        ptr<MemTable> immu_memtable;
        ptr<Version> version_;
        int file_number_;
        LogSeqNumber lsn_;
        int minor_compact(ptr<MemTable> mem);
    public:
        explicit DBImpl(const std::string& dn_name);
        static ptr<DBImpl> open(const std::string& db_name);
        static ptr<DBImpl> create(const std::string& db_name);
        void set(ptr<Slice> key,ptr<Slice> value);
        ptr<Slice> get(ptr<Slice> key);
        void remove(ptr<Slice> key);
    };
}
#endif //MINIDB_DB_IMPL_H
