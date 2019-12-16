//
// Created by cangfeng on 2019/12/7.
//
#include "sstable.h"
#include "format.h"
#include "config.h"
#include "comparator.h"
#include <cassert>
#include <utility>

namespace minidb {
    int SSTable::remove() {
        return reader->remove();
    }

    SSTable::SSTable(const std::string &file_name) {
        reader = make_ptr<MmapReader>(file_name, true);
        int size = reader->size();
        reader->seek(size - 8);
        uint64_t root_block_offset;
        reader->read(&root_block_offset, 8);
        int x;
        reader->read(&x,4);
        min_user_key = make_ptr<Slice>(x);
        reader->read((void *) min_user_key->data(), x);
        reader->read(&x,4);
        max_user_key = make_ptr<Slice>(x);
        reader->read((void*)max_user_key->data(),x);
        root = make_ptr<Block>((char *) (reader->base() + root_block_offset));
    }

    SSTable::SSTable(const std::string &db_name, int file_number) : SSTable(
            db_name + "/" + fn_fmt(file_number) + ".sst") {
        file_number_=file_number;
    }
    int SSTable::file_number() {
        return file_number_;
    }
    ptr<Record> SSTable::lower_bound(ptr<Record> lookup) {
        if(userkey_comparator(lookup->user_key(),min_user_key)<0){
            return nullptr;
        }
        if(userkey_comparator(lookup->user_key(),max_user_key)>0){
            return nullptr;
        }
        ptr<Block> blk = root;
        ptr<Record> ret;
        for (;;) {
            ret = blk->lower_bound(lookup);
            if (ret && ret->type() == KeyType::OFFSET) {
                blk = make_ptr<Block>((char *) (reader->base() + *(uint64_t *) (ret->value()->data())));
            } else {
                break;
            }
        }
        if(ret== nullptr){
            miss_times_++;
        }
        return ret;
    }

    int SSTable::miss_times() {
        return miss_times_;
    }

    bool SSTable::wait_compact() {
        return wait_compact_;
    }

    SSTable::Iterator::Iterator(minidb::ptr<minidb::SSTable> sst) {
        this->sst=sst;
        block_stack.push(sst->root->iterator());
    }
    bool SSTable::Iterator::has_next() {
        return !block_stack.empty();
    }
    ptr<class minidb::Record> SSTable::Iterator::next() {
        for(;;) {
            Block::Iterator &iter = block_stack.top();
            if(iter.hash_next()){
                ptr<Record> ret = iter.next();
                if(ret->type()==KeyType::OFFSET){
                    block_stack.push(Block((char *) (sst->reader->base() + *(uint64_t *) (ret->value()->data()))).iterator());
                }
                else{
                    return ret;
                }
            }else{
                block_stack.pop();
                if(block_stack.empty()){
                    return nullptr;
                }
            }
        }

    }
    Block::Block(char *base) {
        base_ = base;
        record_offset_array_offset = *(uint16_t *) (base + config::BLOCK_SIZE - 4);
        record_offset_array_size = *(uint16_t *) (base + config::BLOCK_SIZE - 2);
        record_offset_array = (uint16_t *) (base + record_offset_array_offset);
    }

    ptr<Record> Block::lower_bound(ptr<class minidb::Record> lookup) {
        int L = 0;
        int R = record_offset_array_size - 1;
        ptr<Record> ret;
        while (L <= R) {
            int M = (L + R) >> 1;
            ptr<Record> record = make_ptr<Record>(record_offset_array[M] + base_, false);
            int cmp = record_comparator(record, lookup);
            if (cmp >= 0) {
                ret = record;
                R = M - 1;
            } else {
                L = M + 1;
            }
        }
        return ret;
    }
    Block::Iterator::Iterator(minidb::ptr<minidb::Block> blk) {
        this->block=blk;
        block=std::move(blk);
        index=0;
    }
    bool Block::Iterator::hash_next() {
        return index<block->record_offset_array_size;
    }
    ptr<class minidb::Record> Block::Iterator::next() {
        assert(index>=block->record_offset_array_size);
        return make_ptr<Record>(block->record_offset_array[index++]+block->base_, false);
    }
}
