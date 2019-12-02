//
// Created by cangfeng on 2019/12/1.
//


#include "skiplist.h"
#include <iostream>
using namespace std;

int func(const std::shared_ptr<string>& a,const std::shared_ptr<string>& b){
    return 0;
}
class Memtable{
    minidb::SkipList<std::shared_ptr<string>> skiplist_;
public:
    Memtable():skiplist_(func){}

};
int main(){
    minidb::SkipList<std::shared_ptr<int>> skiplist(func);
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