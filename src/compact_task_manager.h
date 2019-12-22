//
// Created by cangfeng on 2019/12/22.
//

#ifndef MINIDB_COMPACT_TASK_MANAGER_H
#define MINIDB_COMPACT_TASK_MANAGER_H

#include <array>
#include <mutex>
#include <set>
#include "config.h"
namespace minidb{
    class CompactTaskManager{
        std::mutex mut_;
        std::array<bool,config::SSTABLE_LEVEL> tag_;
        std::set<int> task_;
    public:
        CompactTaskManager(){
//            for(bool & i : tag_){
//                i=false;
//            }
        }
        void set(int p){
            std::unique_lock<std::mutex> lck(mut_);
            task_.insert(p);
        }
        int get(){
            std::unique_lock<std::mutex> lck(mut_);
            if(task_.empty()){
                return -2;
            }
            auto iter = task_.begin();
            int ret = *iter;
            task_.erase(iter);
            return ret;
        }
    };
}
#endif //MINIDB_COMPACT_TASK_MANAGER_H
