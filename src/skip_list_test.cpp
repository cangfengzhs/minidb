//
// Created by cangfeng on 2019/12/1.
//


#include "skiplist.h"
#include <iostream>
using namespace std;

int main(){
    minidb::SkipList<int> skiplist([](int a,int b){
        return a<b?-1:a==b?0:1;
    });
    for(int i=1;i<100;i+=2){
        skiplist.add(i);
    }
    for(int i=0;i<100;i+=2){
        int ret = skiplist.seek(i);
        cout<<i<<"\t"<<ret<<endl;
        assert(ret==i+1);
    }
    return 0;
}