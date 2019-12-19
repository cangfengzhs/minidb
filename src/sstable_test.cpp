//
// Created by cangfeng on 2019/12/7.
//

#include "sstable_builder.h"
#include "sstable.h"
#include <ctime>
#include <cstdio>
#include <cassert>
#include <map>

using namespace std;
using namespace minidb;

void test_write() {
    map<string, string> data;
    for (int i = 0; i < 1000000; i++) {
        data[to_string(i)]=to_string(i*2);
    }
    SSTableBuilder sst_builder("sst_test", 1);
    for(map<string,string>::iterator iter=data.begin();iter!=data.end();iter++){
        LogSeqNumber lsn = 1;
        KeyType type = KeyType::INSERT;
        ptr<Record> record = make_ptr<Record>(make_ptr<Slice>(iter->first), lsn, type, make_ptr<Slice>(iter->second));
        sst_builder.add_record(record);
    }
    sst_builder.finish();
}

void test_read() {
    SSTable sst("sst_test/00000001.sst");
    for (int i = 0; i < 1000000; i+=10) {
        string user_key = to_string(i);
        KeyType type = KeyType::LOOKUP;
        LogSeqNumber lsn = 1;
        ptr<Record> record = make_ptr<Record>(make_ptr<Slice>(user_key), lsn, type, nullptr);
        ptr<Record> ret = sst.lower_bound(record);
        //printf("a:%5s\n", ret->value()->data());
        //printf("b:%s\n", to_string(i * 2).c_str());
        //assert(*(ret->value()) == *(make_ptr<Slice>(to_string(i * 2))));
    }
}

int main() {
    time_t s = time(NULL);
    test_write();
    time_t e = time(NULL);
    printf("%d\n", e - s);
    time_t s2 = time(NULL);
    test_read();
    time_t e2 = time(NULL);
    printf("%d\n", e2 - s2);
    time_t s3 = time(NULL);
    test_read();
    time_t e3 = time(NULL);
    printf("%d\n", e3 - s3);
    return 0;
}