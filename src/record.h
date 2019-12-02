//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_RECORD_H
#define MINIDB_RECORD_H

#include "slice.h"
#include "format.h"

namespace minidb {
    class Record {
        ptr <Slice> user_key_;
        LogSeqNumber lsn_;
        ptr <Slice> value_;
        KeyType type_;
        Checksum checksum_;
    public:
        Record(ptr <Slice> user_key, LogSeqNumber lsn, KeyType type, ptr <Slice> value);
        Record(char *data, bool hash_checksum);
        ptr<Slice> user_key();
        ptr<Slice> value();
        LogSeqNumber lsn();
        KeyType type();
        Checksum checksum();
    };
}
#endif //MINIDB_RECORD_H
