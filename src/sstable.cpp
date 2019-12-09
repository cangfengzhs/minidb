//
// Created by cangfeng on 2019/12/7.
//
#include "sstable.h"
#include "format.h"
#include "config.h"
#include "comparator.h"
#include <cassert>

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
        return ret;
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
}
