//
// Created by cangfeng on 2019/12/2.
//

#include "db.h"
#include "slice.h"
#include <cassert>
#include <iostream>
#include "timer.h"
#include "log.h"
using namespace std;
using namespace minidb;
int main(){
    LOG::log_level=LOG::LogLevel::INFO;
    DB db = DB::create("test_db");
    log_info("start set 1");
    log_warn("test warning");
    log_error("test error");
    log_debug("test debug");
    timer::start("per 100000");
    for(int i=0;i<10000000;i++){
        string key = to_string(i);
        string value = to_string(i*2);
        db.set(make_shared<Slice>(key),make_shared<Slice>(value));
        if(i%100000==99999){
            timer::end("per 100000");
            timer::print();
            timer::start("per 100000");
        }
    }
    log_info("start set 2");
    for(int i=0;i<10000000;i+=2){
        string key = std::to_string(i);
        string value = std::to_string(i*4);
        db.set(make_shared<Slice>(key),make_shared<Slice>(value));
    }
    log_debug("start delete");
    for(int i=0;i<10000000;i+=4){
        if(i==8){
            log_debug("tag:8");
        }
        string key = std::to_string(i);
        db.remove(make_shared<Slice>(key));
    }
    log_debug("start get");
    timer::start("get");
    for(int i=0;i<10000000;i++){
        string key = to_string(i);
        shared_ptr<Slice> value;
        if(i%4==0){
            value = nullptr;
        }
        else if(i%2==0){
            value = make_shared<Slice>(to_string(i*4));
        }
        else{
            value = make_shared<Slice>(to_string(i*2));
        }
        auto ret = db.get(make_shared<Slice>(key));
        if(i%100000==99999){
            timer::end("get");
            timer::print();
            timer::start("get");
        }
        if(ret==value||(ret&&value&&*ret==*value)){
            //cout<<"true"<<endl;
        }else{
            assert(false);
        }
    }
    return 0;
}