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
    int SSTableBuilder::add_index(const minidb::ptr<minidb::Record>& record,int index_level) {
        if(index_block_list.size()<=index_level){
            index_block_list.emplace_back(make_ptr<BlockBuilder>());
        }
        auto& block = index_block_list[index_level];
        int ret = block->add_record(record);
        if(ret==0){
            return 0;
        }
        ptr<Record> index_record = make_index(block);
        block->dump(writer);
        block->clear();
        block->add_record(record);
        add_index(index_record,index_level+1);
        return 0;
    }
    int SSTableBuilder::add_record(const ptr<class minidb::Record>& record) {
        if(min_user_key== nullptr){
            min_user_key=record->user_key();
        }
        max_user_key=record->user_key();
        int ret = data_block->add_record(record);
        if(ret==0){
            return 0;
        }
        ptr<Record> index_record = make_index(data_block);
        data_block->dump(writer);
        data_block->clear();
        data_block->add_record(record);
        add_index(index_record,0);
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
        //记录根节点offset
        uint64_t metadata_offset=writer->size();
        writer->append(&root_offset,8);
        //记录user_key 区间
        int x = min_user_key->size();
        writer->append(&x,4);
        writer->append(min_user_key->data(),x);
        x = max_user_key->size();
        writer->append(&x,4);
        writer->append(max_user_key->data(),x);
        writer->append(&metadata_offset,8);
        //写入magic
        writer->append((char*)(&config::MAGIC),8);
        writer->sync();
        writer->close();
        return 0;
    }
    ptr<Record> SSTableBuilder::make_index(const minidb::ptr<minidb::BlockBuilder>& block) {
        ptr<Record> r = block->max_record();
        uint64_t offset = writer->size();
        char* data = (char*)&offset;
        ptr<Record> index_record = make_ptr<Record>(r->user_key(),r->lsn(),KeyType::OFFSET,make_ptr<Slice>(data,data+8));
        return index_record;
    }

    uint64_t SSTableBuilder::size() {
        return writer->size();
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
    int BlockBuilder::add_record(const ptr<class minidb::Record>& record) {
        int need = record->user_key()->size();
        need+=4+8+sizeof(KeyType)+4+2;
        if(record->value()){
            need+=+record->value()->size();
        }
        if(size_+need>config::BLOCK_SIZE){
            return -1;
        }
        record_list.push_back(record);
        size_+=need;
        return 0;
    }
    int BlockBuilder::clear() {
        record_list.clear();
        size_=4;
        return 0;
    }
    int BlockBuilder::dump(const minidb::ptr<minidb::BufWriter>& writer) {
        uint16_t offset=0;
        vec<uint16_t> record_offset_array;
        for(const auto& record:record_list){
            int cnt=0;
            record_offset_array.push_back(offset);
            int size = record->user_key()->size();
            writer->append(&size,4);
            cnt+=4;
            writer->append(record->user_key()->data(),record->user_key()->size());
            cnt+=record->user_key()->size();
            LogSeqNumber lsn = record->lsn();
            writer->append(&lsn,8);
            cnt+=8;
            KeyType type = record->type();
            writer->append(&type,sizeof(KeyType));
            cnt+=sizeof(KeyType);
            if(record->value()) {
                size = record->value()->size();
                writer->append(&size, 4);
                cnt += 4;
                writer->append(record->value()->data(), record->value()->size());
                cnt += record->value()->size();
            }
            offset+=cnt;
        }
        uint16_t record_offset_array_offset = offset;
        for(auto record_offset:record_offset_array){
            writer->append(&record_offset,2);
            offset+=2;
        }
        std::string padding(config::BLOCK_SIZE-4-offset,0);
        writer->append(padding.data(),padding.size());
        writer->append(&record_offset_array_offset,2);
        uint16_t array_size = record_list.size();
        writer->append(&array_size,2);
        return 0;
    }
    BlockBuilder::BlockBuilder():size_(4){}//block末尾2字节存array的base，2字节存array的size
}