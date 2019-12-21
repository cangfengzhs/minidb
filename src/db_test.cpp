//
// Created by cangfeng on 2019/12/2.
//

#include "db.h"
#include "slice.h"
#include <cassert>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include "timer.h"
#include "log.h"

using namespace std;
using namespace minidb;
int main(){
    LOG::log_level=LOG::LogLevel::INFO;
    string key,value;
    DB db = DB::create("test_db");
    log_info("start set");
    timer::start("per 100000");
    ifstream fin("../scripts/input.txt");
    string line;

    int i=0;
    vector<string> key_list;
    vector<string> value_list;
    while(!fin.eof()){
        key_list.clear();
        value_list.clear();
        for(int i=0;i<100000&&!fin.eof();i++){
            fin>>key>>value;
            key_list.emplace_back(key);
            value_list.emplace_back(value);
        }
        timer::start("per 100000");

        for(int i=0;i<key_list.size();i++){
            if(value_list[i]=="delete"){
                db.remove(make_shared<Slice>(key_list[i]));
            }
            else{
                db.set(make_shared<Slice>(key_list[i]),make_shared<Slice>(value_list[i]));
            }
        }

        timer::end("per 100000");
        timer::print();

        i++;
    }
    fin.close();
    log_debug("start get");
    ifstream fin2("../scripts/output.txt");
    unordered_map<string,string> expire_set;
    while(!fin2.eof()){
        fin2>>key>>value;
        expire_set[key]=value;
    }
    timer::start("get");
    int cnt=0;
    for(auto iter=expire_set.begin();iter!=expire_set.end();iter++){
        cnt++;
        auto ret = db.get(make_shared<Slice>(iter->first));
        if((iter->second=="delete")){
            assert(ret== nullptr);
        }
        else{
            shared_ptr<Slice> expire = make_shared<Slice>(iter->second);
            assert(*ret==*expire);
        }
        if(cnt%10000==0){
            timer::end("get");
            log_info("get %d",cnt);
            timer::print();
            timer::start("get");
        }
    }
    return 0;
}