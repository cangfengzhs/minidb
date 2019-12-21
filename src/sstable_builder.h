//
// Created by cangfeng on 2019/12/2.
//

#ifndef MINIDB_SSTABLE_BUILDER_H
#define MINIDB_SSTABLE_BUILDER_H
#include "format.h"
#include "config.h"
#include "record.h"
#include "file_util.h"
namespace minidb{
    class BlockBuilder;
    class SSTableBuilder{
        ptr<Slice> min_user_key;
        ptr<Slice> max_user_key;
        //多级index
        vec<ptr<BlockBuilder>> index_block_list;
        ptr<BlockBuilder> data_block;
        ptr<BufWriter> writer;
        int add_index(const ptr<Record>& record,int index_level);
        ptr<Record> make_index(const ptr<BlockBuilder>& block);
    public:
        SSTableBuilder(const std::string& db_name,int file_number);
        int add_record(const ptr<Record>& record);
        int finish();
        uint64_t size();
    };
    class BlockBuilder{
        vec<ptr<Record>> record_list;
        int size_;
    public:
        BlockBuilder();
        int add_record(const ptr<Record>& record);
        int size();
        bool empty();
        int dump(const ptr<BufWriter>& writer);
        int clear();
        ptr<Record> max_record();
    };
}
#endif //MINIDB_SSTABLE_BUILDER_H
