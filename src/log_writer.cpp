//
// Created by cangfeng on 2019/12/2.
//

#include "log_writer.h"
#include "file_util.h"
#include <sstream>
#include <unistd.h>
#include <cstdio>
#include <cassert>
namespace minidb{
    LogWriter::LogWriter(const std::string& db_name,int file_number,bool create) {
        file_number_=file_number;
        writer = make_ptr<BufWriter>(db_name+"/"+fn_fmt(file_number)+".log",true, create);
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
        max_lsn_=record->lsn();
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
        writer->append(data,size);
    }
    int LogWriter::flush() {
        return writer->flush();
    }
    int LogWriter::sync() {
        return writer->sync();
    }

    int LogWriter::file_number() {
        return file_number_;
    }

    LogSeqNumber LogWriter::max_lsn() {
        return max_lsn_;
    }

    int LogWriter::remove() {
        return writer->remove();
    }
}