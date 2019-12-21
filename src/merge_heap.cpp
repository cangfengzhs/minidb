//
// Created by cangfeng on 2019/12/19.
//

#include "merge_heap.h"
#include "comparator.h"
#include "timer.h"
#include "debug.h"
namespace minidb {
    void MergeHeap::add_sst(const ptr<class minidb::SSTable> &sst) {
        SSTable::Iterator iter = sst->iterator();
        ptr<Record> record = iter.next();
        heap_array.push_back(std::make_pair(record, iter));
    }

    void MergeHeap::init() {
        for(int i=0;i<heap_array.size();i++){
            iter_end_flag.push_back(false);
        }
    }


    ptr<class minidb::Record> MergeHeap::pop() {
        int index=-1;
        for(int i=0;i<heap_array.size();i++){
            if(iter_end_flag[i]){
                continue;
            }
            if(index==-1||record_comparator(heap_array[index].first,heap_array[i].first)>0){
                index=i;
            }
        }
        auto ret = std::move(heap_array[index].first);
        heap_array[index].first=nullptr;
        if(!heap_array[index].second.has_next()){
            iter_end_flag[index]=true;
        }
        else{
#ifdef DEBUG
            timer::start("sst iter next");
#endif
            heap_array[index].first=std::move(heap_array[index].second.next());
#ifdef DEBUG
            timer::end("sst iter next");
#endif
        }
        return ret;
    }
}