//
// Created by cangfeng on 2019/12/19.
//

#ifndef MINIDB_MERGE_HEAP_H
#define MINIDB_MERGE_HEAP_H

#include "sstable.h"

namespace minidb{
    class MergeHeap{
        using Node = std::pair<ptr<Record>,SSTable::Iterator>;
        bool init_flag= false;
        vec<int> heap_array_index;
        vec<Node> heap_array;
    public:
        void add_sst(const ptr<SSTable>& sst);
        void init();
        bool empty();
        ptr<Record> pop();
    };
}
#endif //MINIDB_MERGE_HEAP_H
