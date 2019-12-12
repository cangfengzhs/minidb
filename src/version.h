//
// Created by cangfeng on 2019/12/4.
//

#ifndef MINIDB_VERSION_H
#define MINIDB_VERSION_H

#include "format.h"
#include "log_writer.h"
#include "memtable.h"
#include "sstable.h"
#include "file_util.h"
#include <set>
#include <memory>
namespace minidb{
    class VersionEdit;
    class DBImpl;
    /* 1. db_impl打开version，并恢复memtable
     * 2. db_impl新建空version
     * 3. db_impl将version_edit加到version上
     */
    class Version{
        FileMeta filemeta;
        ptr<LogWriter> log_;
        ptr<LogWriter> pre_log_;
        vec<ptr<SSTable>> sst_list_;
        LogSeqNumber lsn_;
        friend class VersionEdit;
        friend class DBImpl;
    public:
        //在内存中构建新的version
        Version(ptr<LogWriter> log,ptr<LogWriter> imm_log,vec<ptr<SSTable>> sst_list,LogSeqNumber lsn,const std::string& db_name,int file_number,bool create);
        //新建version并修改version pointer
        ptr<Version> apply(ptr<VersionEdit> edit,const std::string& db_name,int file_number);
        void remove();
    };
    
    class VersionEdit{
        ptr<LogWriter> log_= nullptr;
        ptr<LogWriter> pre_log_= nullptr;
        vec<ptr<SSTable>> add_sst_;
        vec<ptr<SSTable>> remove_sst_;
        friend class Version;
    public:
        VersionEdit()= default;
        void set_log(ptr<LogWriter> log);
        void set_pre_log(ptr<LogWriter> log);
        void add_sst(ptr<SSTable> sst,int level);
        void remove_sst(ptr<SSTable> sst,int level);
    };
}
#endif //MINIDB_VERSION_H
