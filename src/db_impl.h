//
// Created by cangfeng on 2019/12/2.
//

#ifndef MINIDB_DB_IMPL_H
#define MINIDB_DB_IMPL_H

#include "slice.h"
#include "memtable.h"
#include "format.h"
#include "log_writer.h"
#include "sstable.h"
#include "version.h"
#include "concurrent_queue.h"
#include <chrono>
#include <memory>
#include <mutex>
#include <condition_variable>
namespace minidb{
class DBImpl:public std::enable_shared_from_this<DBImpl>{
        std::string db_name_;
        ptr<MemTable> memtable_;
        ptr<MemTable> immu_memtable_;
        ptr<Version> version_;
        int file_number_;
        LogSeqNumber lsn_;
        ConcurrentQueue<int> compact_task_queue;
        //ConcurrentQueue<int> write_task_queue;
        int minor_compact(const ptr<MemTable>& mem);
        int major_compact(int level);
        void make_write_room();
        bool stop_;
        static void _start_compact_thread(ptr<DBImpl> db);
        void write(const ptr<Slice>& user_key,KeyType key_type,const ptr<Slice>& value);
public:
        explicit DBImpl(std::string  dn_name);
        static ptr<DBImpl> open(const std::string& db_name);
        static ptr<DBImpl> create(const std::string& db_name);
        void set(const ptr<Slice>& key,const ptr<Slice>& value);
        ptr<Slice> get(const ptr<Slice>& key);
        void remove(const ptr<Slice>& key);
        void stop();
        void start_compact_thread();
        void do_compact(bool loop);
        ~DBImpl();

    };
}
#endif //MINIDB_DB_IMPL_H
