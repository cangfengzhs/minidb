//
// Created by cangfeng on 2019/12/21.
//

#ifndef MINIDB_WRITE_TASK_H
#define MINIDB_WRITE_TASK_H

#include "format.h"
#include "slice.h"
#include <tuple>
#include <mutex>
#include <condition_variable>

namespace minidb {
    class WriteTask : public vec<std::tuple<ptr < Slice>, KeyType, ptr < Slice>>> {
    std::condition_variable conv;
    bool done_= false;
    public:
    inline void append(const ptr <Slice> &user_key, KeyType key_type, const ptr <Slice> &value);
    inline void extend(const WriteTask &task);
    inline void wait(std::unique_lock<std::mutex>& lck);
    inline void notify();
    inline bool done();
    inline void done(bool d);
};


void WriteTask::append(const ptr<class minidb::Slice> &user_key, enum minidb::KeyType key_type,
                       const ptr<class minidb::Slice> &value) {
    emplace_back(std::make_tuple(user_key, key_type, value));
}

void WriteTask::extend(const class minidb::WriteTask &task) {
    insert(end(), task.begin(), task.end());
}

void WriteTask::wait(std::unique_lock<std::mutex>& lck) {
    conv.wait(lck);
}

void WriteTask::notify() {
    conv.notify_one();
}

bool WriteTask::done() {
    return done_;
}

void WriteTask::done(bool d) {
    done_ = d;
}

}

#endif //MINIDB_WRITE_TASK_H
