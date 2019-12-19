//
// Created by cangfeng on 2019/12/19.
//

#include "merge_heap.h"
#include <cstdio>
#include <iostream>
#include <cstring>

using namespace std;
using namespace minidb;


int main(){
    ptr<SSTable> sst1 = make_ptr<SSTable>("test_db",37);
    ptr<SSTable> sst2 = make_ptr<SSTable>("test_db",41);
    MergeHeap heap;
    heap.add_sst(sst1);
    heap.add_sst(sst2);
    heap.init();
    for(;;){
        auto ret = heap.pop();
        if(strncmp(ret->user_key()->data(),"602253",6)==0){
            break;
        }
    }
    while(!heap.empty()){
        auto ret = heap.pop();
        cout<<ret->user_key()->data()<<endl;
    }
    return 0;

}