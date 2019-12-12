//
// Created by cangfeng on 2019/12/2.
//

#ifndef MINIDB_LOG_WRITER_H
#define MINIDB_LOG_WRITER_H


#include "format.h"
#include "record.h"
#include "config.h"
#include "file_util.h"
#include "memtable.h"
namespace minidb{
    class LogWriter{
        ptr<BufWriter> writer;
        int file_number_;
        LogSeqNumber max_lsn_;
        inline void buf_append(Checksum checksum);
        inline void buf_append(int size);
        inline void buf_append(ptr<Slice> slice);
        inline void buf_append(LogSeqNumber lsn);
        inline void buf_append(KeyType type);
        inline void buf_append(const char* data,int size);
    public:
        LogWriter(const std::string& db_name,int file_number,bool create);
        void append(ptr<Record> record);
        int file_number();
        LogSeqNumber max_lsn();
        int flush();
        int sync();
        int remove();
    };
}
#endif //MINIDB_LOG_WRITER_H
