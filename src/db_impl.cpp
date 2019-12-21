//
// Created by cangfeng on 2019/12/2.
//

#include "db_impl.h"
#include "version.h"
#include "file_util.h"
#include "sstable_builder.h"
#include "timer.h"
#include "merge_heap.h"
#include "log.h"
#include "write_task.h"
#include <condition_variable>
#include <utility>
#include <queue>
#include <thread>
#include <tuple>
#include <thread>
#include <random>
#include "debug.h"
namespace minidb {
    DBImpl::DBImpl(std::string db_name) : db_name_(std::move(db_name)) {
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
        SstSetList sst_set_list;
        //恢复memtable
        reader.read(&x, 4);
        if (x > 0) {
            impl->memtable_ = log2mem(x);
            log = make_ptr<LogWriter>(db_name, x, false);
            impl->file_number_ = max(impl->file_number_.load(), x);
        }
        //恢复immu_memtable
        reader.read(&x, 4);
        if (x > 0) {
            impl->immu_memtable_ = log2mem(x);
            pre_log = make_ptr<LogWriter>(db_name, x, false);
            impl->file_number_ = max(impl->file_number_.load(), x);
        }
        //恢复sst_list
        reader.read(&x,4);
        int cnt = x;
        char level;
        for(int i=0;i<cnt;i++){
            reader.read(&x,4);
            reader.read(&level,1);
            sst_set_list[level].insert(make_ptr<SSTable>(db_name,x));
            impl->file_number_ = max(impl->file_number_.load(), x);
        }
        //获取version的最大lsn，避免出现log都为空的情况
        LogSeqNumber lsn;
        reader.read(&lsn,sizeof(LogSeqNumber));
        impl->lsn_=max(impl->lsn_,lsn);

        //init version
        impl->version_=make_ptr<Version>(log,pre_log,sst_set_list,lsn,db_name,version_fn, false);
        //impl->start_compact_thread();
        if(impl->immu_memtable_){
            impl->compact_task_queue.push(-1);
        }
        return impl;

    }

    ptr<DBImpl> DBImpl::create(const std::string &db_name) {
        create_dir(db_name);
        ptr<DBImpl> impl = make_ptr<DBImpl>(db_name);
        ptr<LogWriter> log = make_ptr<LogWriter>(db_name,impl->file_number_++,true);
        impl->version_ = make_ptr<Version>(log, nullptr,SstSetList(),impl->lsn_,db_name, impl->file_number_.load(), true);
        set_version_pointer(db_name,impl->file_number_);
        impl->file_number_++;
        impl->memtable_ = make_ptr<MemTable>();
        impl->start_compact_thread();
        return impl;
    }

    void DBImpl::set(const minidb::ptr<minidb::Slice>& key, const minidb::ptr<minidb::Slice>& value) {
        WriteTask task;
        task.append(key,KeyType::INSERT,value);
        write(task);
    }
    //只有一个线程会执行make_write_room
    void DBImpl::make_write_room() {
        std::unique_lock<std::mutex> lck(mut_);
        if(memtable_->size()>=config::MEMTABLE_MAX_SIZE){
            while(version_->sst_set_list_[0].size()>=config::SSTABLE_MAX_FILE_COUNT||immu_memtable_!= nullptr||version_->pre_log_!= nullptr){
                lck.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                lck.lock();
            }
            log_debug("make write room");
            immu_memtable_ = memtable_;
            ptr<VersionEdit> edit = make_ptr<VersionEdit>();
            edit->set_pre_log(version_->log_);
            edit->set_log(make_ptr<LogWriter>(db_name_, file_number_++, true));
            int new_ver_fn = file_number_++;
            auto new_ver = version_->apply(edit, db_name_, new_ver_fn);
            exchange_version(new_ver, new_ver_fn);
            memtable_ = make_ptr<MemTable>();
            compact_task_queue.push(-1);
        }
    }
    void DBImpl::write(const minidb::ptr<minidb::Slice>& user_key, minidb::KeyType key_type,
                       const minidb::ptr<minidb::Slice>& value) {

        make_write_room();
        LogSeqNumber lsn = lsn_ + 1;
        ptr<Record> record = make_ptr<Record>(user_key, lsn, key_type, value);
        //timer::start(std::string("log"));
        version_->log_->append(record);
        version_->log_->flush();
        //timer::end(std::string("log"));
        //timer::start(std::string("mem"));
        memtable_->set(user_key, lsn, key_type, value);
        //timer::end(std::string("mem"));
        lsn_ = lsn;
        //do_compact(false);

    }

    void DBImpl::write(WriteTask &task) {
        log_debug("start write");
        std::unique_lock<std::mutex> lck(write_mut_);
        write_task_queue.push_back(&task);
        while (!(&task == write_task_queue.front() || task.done())) {
            task.wait(lck);
        }
        if (task.done()) {
            return;
        }
        //只有一个线程会执行到此处
        auto head = write_task_queue.begin();
        auto tail = write_task_queue.begin();
        WriteTask *last_task = *tail;
        int cnt = 0;
        while (cnt<100) {
            if (tail == write_task_queue.end()) {
                break;
            } else {
                last_task=*tail;
                cnt++;
                tail++;
                continue;
            }
        }
        //释放锁，让其他线程可以将task添加到queue
        {
            lck.unlock();
            log_debug("start make write room");
            make_write_room();
            LogSeqNumber lsn = lsn_ + 1;
            for (auto iter = head; iter != tail; iter++) {
                for (auto &tup : *(*iter)) {
                    ptr<Record> record = make_ptr<Record>(std::get<0>(tup), lsn, std::get<1>(tup), std::get<2>(tup));
                    //timer::start(std::string("log"));
                    version_->log_->append(record);
                    //timer::end(std::string("log"));
                    //timer::start(std::string("mem"));
                    memtable_->set(std::get<0>(tup), lsn, std::get<1>(tup), std::get<2>(tup));
                }
            }
            version_->log_->flush();
            lsn_ = lsn;
            lck.lock();
        }
        while(true){
            WriteTask* t = write_task_queue.front();
            write_task_queue.pop_front();
            t->done(true);
            t->notify();
            if(t==last_task){
                break;
            }
        }
        if(!write_task_queue.empty()) {
            write_task_queue.front()->notify();
        }
    }
    ptr<Slice> DBImpl::get(const minidb::ptr<minidb::Slice>& key) {
        std::unique_lock<std::mutex> lck(mut_);
        auto memtable = memtable_;
        auto immu_memtable = immu_memtable_;
        auto version = version_;
        LogSeqNumber lsn = lsn_;
        lck.unlock();
        //memtable
        ptr<Record> ret = memtable->get(key, lsn);
        if(ret){return ret->type()==KeyType::DELETE? nullptr:ret->value();}
        //immu memtable
        if(immu_memtable){
            ret = immu_memtable->get(key,lsn);
            if(ret){return ret->type()==KeyType::DELETE? nullptr:ret->value();}
        }
        //level 0和level 1-n都要搜索一遍，取lsn较大的
        ptr<Record> lookup = make_ptr<Record>(key,lsn,KeyType::LOOKUP, nullptr);
        auto& sst_set_list = version->sst_set_list_;
        //sst level 0
        ptr<Record> res;
        for(const auto& sst:sst_set_list[0]){
            auto tmp = sst->lower_bound(lookup);
            //TODO incr miss count
            if(!sst->wait_compact()&&sst->miss_times()>=config::MAX_MISS_TIMES){
                //TODO set sst wait compact
                compact_task_queue.push(0);
            }
            if(tmp== nullptr){
                continue;
            }
            if(userkey_comparator(tmp->user_key(),lookup->user_key())==0){
                if(res==nullptr||tmp->lsn()>res->lsn()){
                    res=tmp;
                }
            }
        }
        //sst level 1-n
        ptr<Record> res2;
        for(int i=1;i<sst_set_list.size()&&res2== nullptr;i++){
            for(const auto& sst:sst_set_list[i]){
                auto tmp = sst->lower_bound(lookup);
                //TODO incr miss count
                if(!sst->wait_compact()&&sst->miss_times()>=config::MAX_MISS_TIMES){
                    //TODO set sst wait compact
                    compact_task_queue.push(i);
                }
                if(tmp== nullptr){
                    continue;
                }
                if(userkey_comparator(tmp->user_key(),lookup->user_key())==0){
                    res2 = tmp;
                    break;
                }
            }
        }
        ret = res;
        if(res2){
            if(ret== nullptr){
                ret = res2;
            }else if(res2->lsn()>ret->lsn()){
                ret = res2;
            }
        }
        //do_compact(false);
        if(ret== nullptr||ret->type()==KeyType::DELETE){
            return nullptr;
        }
        return ret->value();
    }

    void DBImpl::remove(const minidb::ptr<minidb::Slice>& key) {
        WriteTask task;
        task.append(key,KeyType::DELETE, nullptr);
        write(task);
    }
    int DBImpl::minor_compact(const minidb::ptr<minidb::MemTable>& mem) {
        int fn;
        ptr<VersionEdit> version_edit;
        std::unique_lock<std::mutex> lck(mut_);
        auto version = version_;
        {
            lck.unlock();
            log_debug("minor compact");
            fn = file_number_++;
            SSTableBuilder sst(db_name_, fn);
            auto iter = mem->iterator();
            while (iter.hash_next()) {
                ptr<Record> record = iter.next();
                sst.add_record(record);
            }
            sst.finish();
            ptr<SSTable> s = make_ptr<SSTable>(db_name_, fn);
            version_edit = make_ptr<VersionEdit>();
            version_edit->set_pre_log(nullptr);
            version_edit->add_sst(s, 0);
            lck.lock();
        }
        version->pre_log_->remove();
        int new_ver_fn = file_number_++;
        auto new_ver = version->apply(version_edit, db_name_, new_ver_fn);
        exchange_version(new_ver, new_ver_fn);
        immu_memtable_ = nullptr;
        set_version_pointer(db_name_, new_ver_fn);
        lck.unlock();
        if(new_ver->sst_set_list_[0].size()>=config::SSTABLE_MAX_FILE_COUNT){
            //lck.unlock();
            major_compact(0);
            //lck.lock();
        }
        if(new_ver->sst_set_list_[0].size()>=config::SSTABLE_SOFT_MAX_FILE_COUNT){
            compact_task_queue.push(0);
        }
        return 0;
    }
    void DBImpl::stop() {
        stop_= true;
        compact_thread.join();
    }
    DBImpl::~DBImpl(){
        if(!stop_){
            stop();
        }
    }
    void DBImpl::start_compact_thread() {
        compact_thread = std::thread(_start_compact_thread,shared_from_this());
    }
    void DBImpl::_start_compact_thread(const minidb::ptr<minidb::DBImpl>& db) {
        db->do_compact(true);
    }
    void DBImpl::do_compact(bool loop) {
        while(!stop_){
            log_debug("compact task number:%d",compact_task_queue.size());
            if(loop&&compact_task_queue.empty()){
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            int last=-2;
            while(!compact_task_queue.empty()){
                int x = compact_task_queue.front();
                log_debug("compact level:%d",x);
                compact_task_queue.pop();
                if(x==-1){
                    minor_compact(immu_memtable_);
                    continue;
                }
                if(x==last){
                    continue;
                }
                else{
                    major_compact(x);
                }
                last=x;
            }
            if(!loop){
                break;
            }
        }
    }
    int DBImpl::major_compact(int level) {
        std::unique_lock<std::mutex> lck(mut_);
        //最后一个level不能compact
        if(level==config::SSTABLE_LEVEL-1){
            return 0;
        }
        log_debug("major compact");
        ptr<Version> version = version_;
        ptr<VersionEdit> edit = make_ptr<VersionEdit>();
        //获取level级要compact的sst
        SSTableSet sst_set{};
        for(const auto& sst:version->sst_set_list_[level]){
            if(sst->wait_compact()){
                sst_set.insert(sst);
                edit->remove_sst(sst,level);
            }
        }
        if(sst_set.empty()){
            int cnt = version->sst_set_list_[level].size()-config::SSTABLE_SOFT_MAX_FILE_COUNT;
            if(cnt<0)cnt=0;
            cnt++;
            int mod = version->sst_set_list_[level].size()-cnt+1;
            int x = rand()%mod;
            auto iter = version->sst_set_list_[level].begin();
            while(x--){
                iter++;
            }
            while(cnt--){
                edit->remove_sst(*iter,level);
                sst_set.insert(*iter);
                iter++;
            }
        }
        //根据level级的sst的key范围，选取level+1级的sst
        for(const auto& sst:version->sst_set_list_[level+1]){
            for(const auto& sst2:sst_set){
                if(userkey_comparator(sst->min_user_key,sst2->max_user_key)==1||
                userkey_comparator(sst->max_user_key,sst2->min_user_key)==-1){
                    continue;
                }
                sst_set.insert(sst);
                edit->remove_sst(sst,level+1);
            }
        }
        //对要合并的sst进行迭代（归并排序）
        {
            lck.unlock();
            MergeHeap heap;
            for (const auto &sst:sst_set) {
                heap.add_sst(sst);
            }
            heap.init();
            int sst_fn = file_number_++;
            ptr<SSTableBuilder> sst_builder = make_ptr<SSTableBuilder>(db_name_, sst_fn);
            ptr<Record> last;
            while (!heap.empty()) {
#ifdef DEBUG
                timer::start("pop");
#endif
                auto nxt = heap.pop();
#ifdef DEBUG
                timer::end("pop");
#endif
                if (nxt == nullptr) {
                    break;
                }
                if (last && userkey_comparator(last->user_key(), nxt->user_key()) == 0) {
                    continue;
                } else {
                    if (sst_builder->size() >= config::SSTABLE_FILE_SIZE[level + 1]) {
                        sst_builder->finish();
                        edit->add_sst(make_ptr<SSTable>(db_name_, sst_fn), level + 1);
                        sst_fn = file_number_++;
                        sst_builder = make_ptr<SSTableBuilder>(db_name_, sst_fn);
                    }
#ifdef DEBUG
                    timer::start("add record");
#endif
                    sst_builder->add_record(nxt);
#ifdef DEBUG
                    timer::end("add record");
#endif
                    last = nxt;
                }
            }
            sst_builder->finish();
            edit->add_sst(make_ptr<SSTable>(db_name_, sst_fn), level + 1);
            lck.lock();
        }
        int new_ver_fn = file_number_++;
        auto new_ver = version_->apply(edit,db_name_,new_ver_fn);
        exchange_version(new_ver,new_ver_fn);
        lck.unlock();
        if(version->sst_set_list_[level+1].size()>=config::SSTABLE_MAX_FILE_COUNT){
            major_compact(level+1);
        }
        if(version_->sst_set_list_[level+1].size()>=config::SSTABLE_SOFT_MAX_FILE_COUNT){
            compact_task_queue.push(level+1);
        }
        return 0;
    }

    int DBImpl::exchange_version(ptr<Version> new_ver, int new_ver_fn) {
        version_->remove();
        version_=std::move(new_ver);
        //version_->print();
        set_version_pointer(db_name_,new_ver_fn);
        return 0;
    }

}
