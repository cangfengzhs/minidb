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
        ptr<Record> get(ptr<Slice> user_key,LogSeqNumber lsn);
        class Iterator{
            SkipList<ptr<Record>>::Iterator iter;
            Iterator(SkipList<ptr<Record>>::Iterator iter);
            friend class MemTable;
        public:
            bool hash_next();
            ptr<Record> next();
        };
        Iterator iterator();
    };

}
#endif //MINIDB_MEMTABLE_H
