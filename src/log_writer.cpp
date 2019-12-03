//
// Created by cangfeng on 2019/12/2.
//

#include "log_writer.h"
#include "file_util.h"
#include <sstream>
#include <unistd.h>
#include <iostream>
#include <cassert>
namespace minidb{
    LogWriter::LogWriter(const std::string& db_name,int file_number) {
        file_name_=db_name+"/"+std::to_string(file_number)+".log";
        std::cout<<file_name_<<std::endl;
        log_fd_ = file_util::create_file(file_name_);
        assert(log_fd_!=-1);
        buf_offset=0;
    }
    int LogWriter::remain() {
        return config::LOG_BUF_SIZE-buf_offset;
    }
    void LogWriter::append(minidb::ptr<minidb::Record> record) {
        buf_append(record->checksum());
        buf_append(record->user_key()->size());
        buf_append(record->user_key());
        buf_append(record->lsn());
        buf_append(record->type());
        if(record->type()!=KeyType::DELETE) {
            buf_append(record->value()->size());
            buf_append(record->value());
        }
    }
    void LogWriter::buf_append(minidb::Checksum checksum) {
        buf_append((char*)&checksum,sizeof(Checksum));
    }
    void LogWriter::buf_append(int size) {
        buf_append((char*)&size,sizeof(int));
    }
    void LogWriter::buf_append(minidb::ptr<minidb::Slice> slice) {
        buf_append(slice->data(),slice->size());
    }
    void LogWriter::buf_append(minidb::LogSeqNumber lsn) {
        buf_append((char*)&lsn,sizeof(LogSeqNumber));
    }
    void LogWriter::buf_append(minidb::KeyType type) {
        buf_append((char*)&type,sizeof(KeyType));
    }
    void LogWriter::buf_append(const char *data, int size) {
        for(int i=0;i<size;i++){
            buf[buf_offset] = data[i];
            buf_offset++;
            if(buf_offset>=config::LOG_BUF_SIZE){
                flush();
            }
        }
    }
    int LogWriter::flush() {
        int ret = write(log_fd_,buf,buf_offset);
        assert(ret==buf_offset);
        if(ret==buf_offset)buf_offset=0;
        return ret;
    }
}