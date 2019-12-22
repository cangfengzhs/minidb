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
        int uncomplete_iter_cnt;
    public:
        void add_sst(const ptr<SSTable>& sst);
        void init();
        inline bool empty();
        inline bool complete();
        ptr<Record> pop();
    };
    bool MergeHeap::empty() {
        return heap_array.empty();
    }
    bool MergeHeap::complete() {
        return uncomplete_iter_cnt==0;
    }
}

#endif //MINIDB_MERGE_HEAP_H
