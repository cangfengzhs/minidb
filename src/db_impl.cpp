//
// Created by cangfeng on 2019/12/2.
//

#include "db_impl.h"
#include "version.h"
#include "file_util.h"
#include "sstable_builder.h"
#include "timer.h"
#include "log.h"
#include <algorithm>

namespace minidb {
    DBImpl::DBImpl(const std::string &db_name) : db_name_(db_name) {
        file_number_ = 1;
        lsn_ = 1;
    }

    ptr<DBImpl> DBImpl::open(const std::string &db_name) {
        using std::max;
        ptr<DBImpl> impl = make_ptr<DBImpl>(db_name);
        int version_fn = get_version_pointer(db_name);
        log_info("load version:%d",version_fn);
        MmapReader reader(db_name + "/" + fn_fmt(version_fn) + ".ver", true);
        auto log2mem = [impl, db_name](int log_fn) {
            log_info("load log file %d",log_fn);
            MmapReader log_reader(db_name + "/" + fn_fmt(log_fn) + ".log", false);
            ptr<MemTable> mem=make_ptr<MemTable>();
            Checksum checksum;
            int user_key_size;
            ptr<Slice> user_key;
            LogSeqNumber lsn;
            KeyType key_type;
            int value_size;
            ptr<Slice> value;
            int cnt;
            while (log_reader.remain() > 0) {
                cnt = log_reader.read(&checksum, sizeof(Checksum));
                if (cnt != sizeof(Checksum))break;
                cnt = log_reader.read(&user_key_size, sizeof(int));
                if (cnt != sizeof(int))break;
                user_key = make_ptr<Slice>(user_key_size);
                cnt = log_reader.read((void *) user_key->data(), user_key_size);
                if (cnt != user_key_size)break;
                cnt = log_reader.read(&lsn, sizeof(LogSeqNumber));
                if (cnt != sizeof(LogSeqNumber))break;
                cnt = log_reader.read(&key_type, sizeof(KeyType));
                if (cnt != sizeof(KeyType))break;
                if(key_type==KeyType::INSERT) {
                    cnt = log_reader.read(&value_size, sizeof(int));
                    if (cnt != sizeof(int))break;
                    value = make_ptr<Slice>(value_size);
                    cnt = log_reader.read((void *) value->data(), value_size);
                    if (cnt != value_size)break;
                }
                else{
                    value= nullptr;
                }
                ptr<Record> record = make_ptr<Record>(user_key, lsn, key_type, value);
                if (record->checksum() != checksum)break;
                impl->lsn_ = lsn;
                mem->set(user_key, lsn, key_type, value);
            }
            log_debug("remain:%d",log_reader.remain());
            assert(log_reader.remain()==0);
            return mem;
        };
        int x;
        ptr<LogWriter> log;
        ptr<LogWriter> pre_log;
        vec<ptr<SSTable>> sst_list;
        //恢复memtable
        reader.read(&x, 4);
        if (x > 0) {
            impl->memtable_ = log2mem(x);
            log = make_ptr<LogWriter>(db_name, x, false);
            impl->file_number_ = max(impl->file_number_, x);
        }
        //恢复immu_memtable
        reader.read(&x, 4);
        if (x > 0) {
            impl->immu_memtable = log2mem(x);
            pre_log = make_ptr<LogWriter>(db_name, x, false);
            impl->file_number_ = max(impl->file_number_, x);
        }
        //恢复sst_list
        reader.read(&x,4);
        int cnt = x;
        for(int i=0;i<cnt;i++){
            reader.read(&x,4);
            sst_list.emplace_back(make_ptr<SSTable>(db_name,x));
            impl->file_number_ = max(impl->file_number_, x);
            reader.read(&x,1);
        }
        //获取version的最大lsn，避免出现log都为空的情况
        LogSeqNumber lsn;
        reader.read(&lsn,sizeof(LogSeqNumber));
        impl->lsn_=max(impl->lsn_,lsn);

        //init version
        impl->version_=make_ptr<Version>(log,pre_log,sst_list,lsn,db_name,version_fn, false);
        return impl;

    }

    ptr<DBImpl> DBImpl::create(const std::string &db_name) {
        create_dir(db_name);
        ptr<DBImpl> impl = make_ptr<DBImpl>(db_name);
        ptr<LogWriter> log = make_ptr<LogWriter>(db_name,impl->file_number_++,true);
        impl->version_ = make_ptr<Version>(log, nullptr,vec<ptr<SSTable>>(),impl->lsn_,db_name, impl->file_number_, true);
        set_version_pointer(db_name,impl->file_number_);
        impl->file_number_++;
        impl->memtable_ = make_ptr<MemTable>();
        return impl;
    }

    void DBImpl::set(minidb::ptr<minidb::Slice> key, minidb::ptr<minidb::Slice> value) {
        LogSeqNumber lsn = lsn_ + 1;
        ptr<Record> record = make_ptr<Record>(key, lsn, KeyType::INSERT, value);
        timer::start(std::string("log"));
        version_->log_->append(record);
        version_->log_->flush();
        //version_->log_->sync();
        timer::end(std::string("log"));
        timer::start(std::string("mem"));
        memtable_->set(key, lsn, KeyType::INSERT, value);
        timer::end(std::string("mem"));
        lsn_ = lsn;
        if(memtable_->size()>=config::MEMTABLE_MAX_SIZE){
            minor_compact(memtable_);
            auto new_mem = make_ptr<MemTable>();
            memtable_= nullptr;
            memtable_=new_mem;
        }
    }

    ptr<Slice> DBImpl::get(minidb::ptr<minidb::Slice> key) {
        LogSeqNumber lsn = lsn_;
        ptr<Slice> ret = memtable_->get(key, lsn);
        if(ret==nullptr){
            ptr<Record> res;
            ptr<Record> lookup = make_ptr<Record>(key,lsn,KeyType::LOOKUP, nullptr);
            for(auto sst:version_->sst_list_){
                auto tmp = sst->lower_bound(lookup);
                if(tmp&&userkey_comparator(tmp->user_key(),key)==0){
                    if(res== nullptr){
                        res = tmp;
                    }
                    else if(tmp->lsn()>res->lsn()){
                        res=tmp;
                    }
                }
            }
            if(res){
                ret=res->value();
            }
        }
        if(ret->size()==0){
            ret= nullptr;
        }
        return ret;
    }

    void DBImpl::remove(minidb::ptr<minidb::Slice> key) {
        LogSeqNumber lsn = lsn_ + 1;
        ptr<Record> record = make_ptr<Record>(key, lsn, KeyType::DELETE, nullptr);
        version_->log_->append(record);
        version_->log_->flush();
        //version_->log_->sync();
        memtable_->set(key, lsn, KeyType::DELETE, nullptr);
        lsn_ = lsn;
    }
    int DBImpl::minor_compact(minidb::ptr<minidb::MemTable> mem) {
        printf("minor compact\n");

        int fn = file_number_++;
        SSTableBuilder sst(db_name_,fn);
        auto iter = mem->iterator();
        while(iter.hash_next()){
            ptr<Record> record = iter.next();
            sst.add_record(record);
        }
        sst.finish();

        version_->log_->remove();
        ptr<SSTable> s = make_ptr<SSTable>(db_name_,fn);
        ptr<VersionEdit> version_edit=make_ptr<VersionEdit>();
        version_edit->set_log(make_ptr<LogWriter>(db_name_,file_number_++, true));
        version_edit->add_sst(s,0);
        fn = file_number_++;
        version_->remove();
        version_=version_->apply(version_edit,db_name_,fn);
        set_version_pointer(db_name_,fn);
        return 0;
    }

}
