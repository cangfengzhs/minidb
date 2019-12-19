//
// Created by cangfeng on 2019/12/1.
//


#include "skiplist.h"
#include <iostream>
using namespace std;

int func(const std::shared_ptr<int>& a,const std::shared_ptr<int>& b){
    if(a<b)return -1;
    if(a==b)return 0;
    if(a>b)return 1;
}
int main(){
    minidb::SkipList<std::shared_ptr<int>> skiplist(func);
    for(int i=1;i<100;i+=2){
        skiplist.add(make_shared<int>(i));
    }
    for(int i=0;i<100;i+=2){
        auto ret = skiplist.seek(make_shared<int>(i));
        cout<<i<<"\t"<<ret<<endl;
        assert(*ret==i+1);
    }
    return 0;
}