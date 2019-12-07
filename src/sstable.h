//
// Created by cangfeng on 2019/12/7.
//

#ifndef MINIDB_SSTABLE_H
#define MINIDB_SSTABLE_H

#include "file_util.h"
#include "record.h"

namespace minidb{
    class Block;
    class SSTable{
        ptr<MmapReader> reader;
        ptr<Block> root;
    public:
        SSTable(const std::string& file_name);
        ptr<Record> lower_bound(ptr<Record> lookup);
        int remove();
    };
    class Block{
        char* base_;
        uint16_t* record_offset_array;
        uint16_t record_offset_array_offset;
        uint16_t record_offset_array_size;
    public:
        Block(char* base);
        ptr<Record> lower_bound(ptr<Record> lookup);

    };
}
#endif //MINIDB_SSTABLE_H
