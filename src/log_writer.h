//
// Created by cangfeng on 2019/12/2.
//

#ifndef MINIDB_LOG_WRITER_H
#define MINIDB_LOG_WRITER_H


#include "format.h"
#include "record.h"
#include "config.h"
namespace minidb{
    class LogWriter{
        int log_fd_;
        std::string file_name_;
        char buf[config::LOG_BUF_SIZE];
        int buf_offset;
        inline int remain();
        inline void buf_append(Checksum checksum);
        inline void buf_append(int size);
        inline void buf_append(ptr<Slice> slice);
        inline void buf_append(LogSeqNumber lsn);
        inline void buf_append(KeyType type);
        inline void buf_append(const char* data,int size);
    public:
        LogWriter(const std::string& db_name,int file_number);
        void append(ptr<Record> record);
        int flush();
    };
}
#endif //MINIDB_LOG_WRITER_H
