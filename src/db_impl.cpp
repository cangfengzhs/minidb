//
// Created by cangfeng on 2019/12/2.
//

#include "db_impl.h"
#include "file_util.h"
namespace minidb{
    DBImpl::DBImpl(const std::string &db_name):db_name_(db_name) {
        memtable_ = make_ptr<MemTable>();
        immu_memtabel_= nullptr;
        log_ = make_ptr<LogWriter>(db_name,0);
        file_number_=1;
        lsn_ = 0;
    }
    ptr<DBImpl> DBImpl::open(const std::string &db_name) {
        ptr<DBImpl> impl = make_ptr<DBImpl>(db_name);
        return impl;
    }
    ptr<DBImpl> DBImpl::create(const std::string &db_name) {
        file_util::create_dir(db_name);
        ptr<DBImpl> impl = make_ptr<DBImpl>(db_name);
        return impl;
    }
    void DBImpl::set(minidb::ptr<minidb::Slice> key, minidb::ptr<minidb::Slice> value) {
        LogSeqNumber lsn = lsn_+1;
        ptr<Record> record = make_ptr<Record>(key,lsn,KeyType::INSERT,value);
        log_->append(record);
        memtable_->set(key,lsn,KeyType::INSERT,value);
        lsn_=lsn;
    }
    ptr<Slice> DBImpl::get(minidb::ptr<minidb::Slice> key) {
        LogSeqNumber lsn = lsn_;
        auto ret = memtable_->get(key,lsn);
        return ret;
    }
    void DBImpl::remove(minidb::ptr<minidb::Slice> key) {
        LogSeqNumber lsn = lsn_+1;
        ptr<Record> record = make_ptr<Record>(key,lsn,KeyType::DELETE, nullptr);
        log_->append(record);
        memtable_->set(key,lsn,KeyType::DELETE, nullptr);
        lsn_ = lsn;
    }

}
