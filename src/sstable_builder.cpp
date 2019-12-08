//
// Created by cangfeng on 2019/12/7.
//

#include "sstable_builder.h"
#include "file_util.h"

namespace minidb{
    SSTableBuilder::SSTableBuilder(const std::string &db_name, int file_number) {
        std::string file_name = db_name+"/"+fn_fmt(file_number)+".sst";
        writer = make_ptr<BufWriter>(file_name,true,true);
        data_block = make_ptr<BlockBuilder>();
    }
    int SSTableBuilder::add_index(minidb::ptr<minidb::Record> record,int index_level) {
        if(index_block_list.size()<=index_level){
            index_block_list.emplace_back(make_ptr<BlockBuilder>());
        }
        auto block = index_block_list[index_level];
        int ret = block->add_record(record);
        if(ret==0){
            return 0;
        }
        ptr<Record> index_record = make_index(block);
        add_index(index_record,index_level+1);
        block->dump(writer);
        block->clear();
        block->add_record(record);
        return 0;
    }
    int SSTableBuilder::add_record(ptr<class minidb::Record> record) {
        int ret = data_block->add_record(record);
        if(ret==0){
            return 0;
        }
        ptr<Record> index_record = make_index(data_block);
        add_index(index_record,0);
        data_block->dump(writer);
        data_block->clear();
        data_block->add_record(record);
        return 0;
    }
    int SSTableBuilder::finish() {
        uint64_t root_offset;
        auto index_record = make_index(data_block);
        add_index(index_record,0);
        data_block->dump(writer);
        for(int i=0;i<index_block_list.size();i++){
            auto block= index_block_list[i];
            if(block->empty()){
                continue;
            }
            if(i==index_block_list.size()-1){
                root_offset = writer->size();
                block->dump(writer);
            }
            else{
                index_record = make_index(block);
                add_index(index_record,i+1);
                block->dump(writer);
            }
        }
        writer->write(&root_offset,8);
        writer->write((char*)(&config::MAGIC),8);
        writer->sync();
        writer->close();
        return 0;
    }
    ptr<Record> SSTableBuilder::make_index(minidb::ptr<minidb::BlockBuilder> block) {
        ptr<Record> r = block->max_record();
        uint64_t offset = writer->size();
        char* data = (char*)&offset;
        ptr<Record> index_record = make_ptr<Record>(r->user_key(),r->lsn(),KeyType::OFFSET,make_ptr<Slice>(data,data+8));
        return index_record;
    }
    bool BlockBuilder::empty() {
        return record_list.empty();
    }
    ptr<class minidb::Record> BlockBuilder::max_record() {
        return record_list.back();
    }
    int BlockBuilder::size() {
        return size_;
    }
    int BlockBuilder::add_record(ptr<class minidb::Record> record) {
        int need = record->user_key()->size()+record->value()->size();
        need+=4+8+1+4+2;
        if(size_+need>config::BLOCK_SIZE){
            return -1;
        }
        record_list.push_back(record);
        size_+=need;
        return 0;
    }
    int BlockBuilder::clear() {
        record_list.clear();
        return 0;
    }
    int BlockBuilder::dump(minidb::ptr<minidb::BufWriter> writer) {
        uint16_t offset=0;
        vec<uint16_t> record_offset_array;
        for(auto record:record_list){
            int cnt=0;
            record_offset_array.push_back(offset);
            int size = record->user_key()->size();
            writer->write(&size,4);
            cnt+=4;
            writer->write(record->user_key()->data(),record->user_key()->size());
            cnt+=record->user_key()->size();
            LogSeqNumber lsn = record->lsn();
            writer->write(&lsn,8);
            cnt+=8;
            KeyType type = record->type();
            writer->write(&type,sizeof(KeyType));
            cnt+=sizeof(KeyType);
            size = record->value()->size();
            writer->write(&size,4);
            cnt+=4;
            writer->write(record->value()->data(),record->value()->size());
            cnt+=record->value()->size();
            offset+=cnt;
        }
        uint16_t record_offset_array_offset = offset;
        for(auto record_offset:record_offset_array){
            writer->write(&record_offset,2);
            offset+=2;
        }
        std::string padding(config::BLOCK_SIZE-4-offset,0);
        writer->write(padding.data(),padding.size());
        writer->write(&record_offset_array_offset,2);
        uint16_t array_size = record_list.size();
        writer->write(&array_size,2);
        return 0;
    }
    BlockBuilder::BlockBuilder():size_(4){}//block末尾2字节存array的base，2字节存array的size
}