//
// Created by cangfeng on 2019/12/15.
//

#ifndef MINIDB_CONCURRENT_QUEUE_H
#define MINIDB_CONCURRENT_QUEUE_H

#include <atomic>
#include <queue>
#include <mutex>
namespace minidb{
    template <typename Key>
    class ConcurrentQueue{
        std::mutex mut;
        std::queue<Key> queue_;
    public:
        ConcurrentQueue():queue_(){}
        void push(const Key& key){
            std::unique_lock lck(mut);
            queue_.push(key);
        }
        Key& front(){
            std::unique_lock lck(mut);
            return queue_.front();
        }
        void pop(){
            std::unique_lock lck(mut);
            queue_.pop();
        }
        bool empty(){
            return queue_.empty();
        }
    };
}
#endif //MINIDB_CONCURRENT_QUEUE_H
