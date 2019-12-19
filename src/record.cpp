//
// Created by cangfeng on 2019/12/2.
//

#include "record.h"

#include <utility>
namespace minidb{
    Record::Record(char *data, bool hash_checksum) {
        if(hash_checksum){
            checksum_ = *(Checksum*)data;
            data+= sizeof(Checksum);
        }
        int user_key_size = *(int*)data;
        data+=4;
        user_key_ = make_ptr<Slice>(data,data+user_key_size);
        data+=user_key_size;
        lsn_ = *(LogSeqNumber*)data;
        data+=8;
        type_ =*(KeyType*)data;
        data+=sizeof(KeyType);
        if(type_!=KeyType::DELETE) {
            int value_key_size = *(int *) data;
            data += 4;
            value_ = make_ptr<Slice>(data, data + value_key_size);
        }
    }
    Record::Record(minidb::ptr<minidb::Slice> user_key, minidb::LogSeqNumber lsn, minidb::KeyType type,
                   minidb::ptr<minidb::Slice> value):
                   user_key_(user_key),lsn_(lsn),type_(type),value_(value),checksum_(0){}
    ptr<Slice> Record::value() {
        return value_;
    }
    KeyType Record::type() {
        return type_;
    }
    ptr<Slice> Record::user_key() {
        return user_key_;
    }
    LogSeqNumber Record::lsn() {
        return lsn_;
    }
    Checksum Record::checksum() {
        //TODO 计算checksum
        return checksum_;
    }
}