//
// Created by cangfeng on 2019/12/1.
//

#include "memtable.h"
#include "comparator.h"
#include "timer.h"
#include "error.h"
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
        timer::start("skiplist add");
        skiplist_.add(record);
        timer::end("skiplist add");
    }
    ptr<Record> MemTable::get(minidb::ptr<minidb::Slice> user_key, minidb::LogSeqNumber lsn) {
        ptr<Record> record = make_ptr<Record>(user_key,lsn,KeyType::LOOKUP, nullptr);
        ptr<Record> ret_record;
        try {
            ret_record = skiplist_.seek(record);
        }catch(const KeyNotFound<ptr<Record>>& err){
            ret_record= nullptr;
        }
        if(ret_record && userkey_comparator(ret_record->user_key(),user_key)==0){
            return ret_record;
        }
        return nullptr;
    }
    MemTable::Iterator MemTable::iterator() {
        return Iterator(skiplist_.iterator());
    }
    MemTable::Iterator::Iterator(minidb::SkipList<minidb::ptr<minidb::Record>>::Iterator it):iter(it) {}
    bool MemTable::Iterator::hash_next() {
        return iter.hash_next();
    }
    ptr<class minidb::Record> MemTable::Iterator::next() {
        return iter.next();
    }
}