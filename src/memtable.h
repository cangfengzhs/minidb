//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_MEMTABLE_H
#define MINIDB_MEMTABLE_H

#include "skiplist.h"
#include "slice.h"
#include "record.h"
#include "comparator.h"
namespace minidb{
    class MemTable{
        SkipList<ptr<Record>> skiplist_;
        int size_;
    public:
        MemTable();
        int size();
        void set(ptr<Slice> user_key,LogSeqNumber lsn,KeyType type,ptr<Slice> value);
        ptr<Slice> get(ptr<Slice> user_key,LogSeqNumber lsn);
    };

}
#endif //MINIDB_MEMTABLE_H
