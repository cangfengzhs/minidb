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
#include <array>
#include <unordered_set>
namespace minidb{
    struct SSTableHasher{
        size_t operator()(const ptr<SSTable>& sst)const {
            return sst->file_number();
        }
    };
    struct SSTableEqual{
        bool operator()(const ptr<SSTable>& a,const ptr<SSTable>& b) const{
            return a->file_number()==b->file_number();
        }
    };
    using SSTableSet = std::unordered_set<ptr<SSTable>,SSTableHasher,SSTableEqual>;
    using SstSetList = std::array<SSTableSet,config::SSTABLE_LEVEL>;
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
        SstSetList sst_set_list_;
        LogSeqNumber lsn_;
        friend class VersionEdit;
        friend class DBImpl;
    public:
        //在内存中构建新的version
        Version(ptr<LogWriter> log,ptr<LogWriter> imm_log,SstSetList& sst_set_list,LogSeqNumber lsn,const std::string& db_name,int file_number,bool create);
        //新建version并修改version pointer
        ptr<Version> apply(const ptr<VersionEdit>& edit,const std::string& db_name,int file_number);
        void remove();
        void print();
    };
    
    class VersionEdit{
        ptr<LogWriter> log_= nullptr;
        ptr<LogWriter> pre_log_= nullptr;
        bool log_flag=false;
        bool pre_log_flag= false;
        SstSetList add_sst_;
        SstSetList remove_sst_;
        friend class Version;
    public:
        VersionEdit()= default;
        void set_log(ptr<LogWriter> log);
        void set_pre_log(ptr<LogWriter> log);
        void add_sst(const ptr<SSTable>& sst,int level);
        void remove_sst(const ptr<SSTable>& sst,int level);
    };
}
#endif //MINIDB_VERSION_H
