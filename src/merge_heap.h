//
// Created by cangfeng on 2019/12/19.
//

#ifndef MINIDB_MERGE_HEAP_H
#define MINIDB_MERGE_HEAP_H

#include "sstable.h"

namespace minidb{
    class MergeHeap{
        using Node = std::pair<ptr<Record>,SSTable::Iterator>;
        vec<Node> heap_array;
        vec<bool> iter_end_flag;
    public:
        void add_sst(const ptr<SSTable>& sst);
        void init();
        inline bool empty();
        ptr<Record> pop();
    };
    bool MergeHeap::empty() {
        if(heap_array.empty()){
            return true;
        }
        for(auto flag:iter_end_flag){
            if(!flag){
                return false;
            }
        }
        return true;
    }
}

#endif //MINIDB_MERGE_HEAP_H
