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

    DB db = DB::create("test_db");
    log_info("start set 1");
    log_warn("test warning");
    log_error("test error");
    log_debug("test debug");
    for(int i=0;i<1000000;i++){
        string key = to_string(i);
        string value = to_string(i*2);
        db.set(make_shared<Slice>(key),make_shared<Slice>(value));
        if(i%100000==0){
            timer::print();
        }
    }
    log_info("start set 2");
    for(int i=0;i<1000000;i+=2){
        string key = std::to_string(i);
        string value = std::to_string(i*4);
        db.set(make_shared<Slice>(key),make_shared<Slice>(value));
    }
    cout<<"start delete\n";
    for(int i=0;i<1000000;i+=4){
        cout<<"delete "<<i<<endl;
        string key = std::to_string(i);
        db.remove(make_shared<Slice>(key));
    }
    cout << "start get\n";
    for(int i=0;i<1000000;i++){
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
        cout<<i<<":";
        auto ret = db.get(make_shared<Slice>(key));
        if(ret==value||(ret&&value&&*ret==*value)){
            cout<<"true"<<endl;
        }else{
            cout<<"false"<<endl;
            cout<<ret->data()<<endl;
            cout<<value->data()<<endl;
        }
    }
    return 0;
}