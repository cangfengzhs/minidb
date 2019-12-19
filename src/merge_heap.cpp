//
// Created by cangfeng on 2019/12/19.
//

#include "merge_heap.h"
#include "comparator.h"
#include "log.h"
#include <cassert>

namespace minidb {
    void MergeHeap::add_sst(const ptr<class minidb::SSTable> &sst) {
        assert(!init_flag);
        SSTable::Iterator iter = sst->iterator();
        ptr<Record> record = iter.next();
        heap_array.push_back(std::make_pair(record, iter));
    }

    void MergeHeap::init() {
        init_flag = true;
        for (int i = 0; i < heap_array.size(); i++) {
            heap_array_index.push_back(i);
        }
        for (int i = 1; i < heap_array_index.size(); i++) {
            int x = i;
            int child = heap_array_index[x];
            int parent = heap_array_index[(x - 1) / 2];
            while (x > 0 && record_comparator(heap_array[child].first, heap_array[parent].first) < 0) {
                std::swap(heap_array_index[x], heap_array_index[(x - 1) / 2]);
                x = (x - 1) / 2;
                if (x <= 0) {
                    break;
                }
                child = heap_array_index[x];
                parent = heap_array_index[(x - 1) / 2];
            }
        }
    }

    bool MergeHeap::empty() {
        return heap_array.empty()||heap_array[heap_array_index[0]].first == nullptr;
    }

    ptr<class minidb::Record> MergeHeap::pop() {
        ptr<Record> ret = heap_array[heap_array_index[0]].first;
        if (heap_array[heap_array_index[0]].second.has_next()) {
            heap_array[heap_array_index[0]].first = heap_array[heap_array_index[0]].second.next();
        } else {
            heap_array[heap_array_index[0]].first = nullptr;
        }
        //maintain
        int parent = 0;
        for (;;) {
            int to_swap = parent;
            int x = parent*2+1;
            if(x<heap_array_index.size()&&heap_array[heap_array_index[x]].first!= nullptr) {
                if (heap_array[heap_array_index[parent]].first==nullptr||record_comparator(heap_array[heap_array_index[parent]].first,
                                      heap_array[heap_array_index[x]].first) > 0) {

                    to_swap = x;
                }
            }
            x = parent*2+2;
            if(x<heap_array_index.size()&&heap_array[heap_array_index[x]].first!= nullptr){
                if (heap_array[heap_array_index[to_swap]].first== nullptr||record_comparator(heap_array[heap_array_index[to_swap]].first,
                                      heap_array[heap_array_index[x]].first) > 0) {
                    to_swap = x;
                }
            }
            if(to_swap==parent){
                break;
            }
            std::swap(heap_array_index[parent],heap_array_index[to_swap]);
            parent = to_swap;
        }
        return ret;
    }
}