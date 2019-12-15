//
// Created by cangfeng on 2019/12/7.
//

#ifndef MINIDB_SSTABLE_H
#define MINIDB_SSTABLE_H

#include "file_util.h"
#include "record.h"
#include <stack>
#include <memory>
namespace minidb{
    class Block:public std::enable_shared_from_this<Block>{
        char* base_;
        uint16_t* record_offset_array;
        uint16_t record_offset_array_offset;
        uint16_t record_offset_array_size;
    public:
        Block(char* base);
        ptr<Record> lower_bound(ptr<Record> lookup);
        class Iterator{
            ptr<Block> block;
            int index;
            Iterator(ptr<Block> blk);
            friend class Block;
        public:
            bool hash_next();
            ptr<Record> next();
        };
        Iterator iterator(){
            return Iterator(this->shared_from_this());
        }

    };
    class SSTable:public std::enable_shared_from_this<SSTable>{
        ptr<MmapReader> reader;
        ptr<Block> root;
        int file_number_;
        int miss_times_=0;
        SSTable(const std::string& file_name);
    public:
        ptr<Record> min_record;
        ptr<Record> max_record;
        int file_number();
        SSTable(const std::string& db_name,int file_number);
        ptr<Record> lower_bound(ptr<Record> lookup);
        int remove();
        class Iterator{
            ptr<SSTable> sst;
            std::stack<Block::Iterator> block_stack;
            Iterator(ptr<SSTable> sst);
            friend class SSTable;
        public:
            bool has_next();
            ptr<Record> next();
        };
        Iterator iterator(){
            return Iterator(this->shared_from_this());
        }
    };

}
#endif //MINIDB_SSTABLE_H
