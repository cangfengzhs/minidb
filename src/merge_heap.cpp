//
// Created by cangfeng on 2019/12/19.
//

#include "merge_heap.h"
#include "comparator.h"

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
        ptr<Record> ret= nullptr;
        for(int i=0;i<heap_array.size();i++){
            if(iter_end_flag[i]){
                continue;
            }
            if(ret== nullptr||record_comparator(ret,heap_array[i].first)>0){
                ret = heap_array[i].first;
                index=i;
            }
        }
        heap_array[index].first=nullptr;
        if(!heap_array[index].second.has_next()){
            iter_end_flag[index]=true;
        }
        else{
            heap_array[index].first=heap_array[index].second.next();
        }
        return ret;
    }
}