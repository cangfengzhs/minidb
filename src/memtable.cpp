//
// Created by cangfeng on 2019/12/1.
//

#include "memtable.h"
#include "comparator.h"
namespace minidb {
    MemTable::MemTable() : size_(0), skiplist_(record_comparator) {}

    int MemTable::size() {
        return size_;
    }

    void MemTable::set(minidb::ptr<minidb::Slice> user_key, minidb::LogSeqNumber lsn, minidb::KeyType type,
                       minidb::ptr<minidb::Slice> value) {
        size_+=user_key->size();
        size_+=9;
        if(value){
            size_+=value->size();
        }
        ptr<Record> record = make_ptr<Record>(user_key,lsn,type,value);
        skiplist_.add(record);
    }
    ptr<Slice> MemTable::get(minidb::ptr<minidb::Slice> user_key, minidb::LogSeqNumber lsn) {
        ptr<Record> record = make_ptr<Record>(user_key,lsn,KeyType::LOOKUP, nullptr);
        auto ret_record = skiplist_.seek(record);
        if(ret_record && ret_record->type()==KeyType::INSERT &&userkey_comparator(ret_record->user_key(),user_key)==0){
            return ret_record->value();
        }
        return nullptr;
    }
}