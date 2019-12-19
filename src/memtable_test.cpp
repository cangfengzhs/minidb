//
// Created by cangfeng on 2019/12/1.
//

#include "memtable.h"
#include "slice.h"
#include "format.h"
using namespace minidb;
using namespace std;
int main(){
    MemTable memTable;
    memTable.set(make_ptr<Slice>("123"),1,KeyType::INSERT,make_ptr<Slice>("234"));
    memTable.set(make_ptr<Slice>("abc"),1,KeyType::INSERT,make_ptr<Slice>("bcd"));
    memTable.set(make_ptr<Slice>("123"),2,KeyType::DELETE, nullptr);
    memTable.set(make_ptr<Slice>("abc"),3,KeyType::INSERT,make_ptr<Slice>("asdfg"));
    memTable.set(make_ptr<Slice>("123"),3,KeyType::INSERT,make_ptr<Slice>("345"));


    auto ret = memTable.get(make_ptr<Slice>("123"),3);
    cout<<ret->data()<<endl;
    ret= memTable.get(make_ptr<Slice>("abc"),3);
    cout<<ret->data()<<endl;
    return 0;


}

