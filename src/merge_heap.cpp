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
        for(int i=0;i<heap_array.size();i++){
            int c = i;
            while(c>0){
                int p = (c-1)/2;
                if(record_comparator(heap_array[c].first,heap_array[p].first)<0){
                    std::swap(heap_array[c],heap_array[p]);
                    c = p;
                }
                else{
                    break;
                }
            }
        }
        uncomplete_iter_cnt=iter_end_flag.size();
    }


    ptr<class minidb::Record> MergeHeap::pop() {
        auto ret = std::move(heap_array[0].first);
        if(heap_array[0].second.has_next()){
            heap_array[0].first=heap_array[0].second.next();
        }
        else{
            uncomplete_iter_cnt--;
            heap_array[0].first= nullptr;
        }
        int p=0;
        while(p<heap_array.size()){
            int chg=-1;
            int lc = (p*2)+1;
            int rc = (p*2)+2;
            if(lc>=heap_array.size()||heap_array[lc].first== nullptr){
                lc=-1;
            }
            if(rc>=heap_array.size()||heap_array[rc].first== nullptr){
                rc=-1;
            }
            if(lc>=0&&rc>=0){
                int cmp = record_comparator(heap_array[lc].first,heap_array[rc].first);
                if(cmp<0){
                    chg=lc;
                }
            }else if(lc>=0){
                chg=lc;
            }else if(rc>=0){
                chg=rc;
            }
            if(chg>=0){
                if(heap_array[p].first==nullptr||record_comparator(heap_array[p].first,heap_array[chg].first)>0){
                    std::swap(heap_array[p],heap_array[chg]);
                    p=chg;
                }
                else{
                    break;
                }
            }
            else{
                break;
            }
        }
        return ret;
    }
}