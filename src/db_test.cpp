//
// Created by cangfeng on 2019/12/2.
//

#include "db.h"
#include "slice.h"
#include <cassert>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <thread>
#include <queue>
#include <mutex>
#include <atomic>
#include "timer.h"
#include "log.h"

using namespace std;
using namespace minidb;

void func(DB& db,const string& file){
    cout<<"start thread "<<this_thread::get_id()<<endl;
    ifstream fin(file);
    ofstream fout(file+".log");
    string key,value;
    int cnt=0;
    while(!fin.eof()){
        fin>>key>>value;
        fout<<key<<endl;
        if(value=="delete"){
            db.remove(make_shared<Slice>(key));
        }
        else{
            db.set(make_shared<Slice>(key),make_shared<Slice>(value));
        }
        cnt++;
        if(cnt%10000==0){
            cout<<this_thread::get_id()<<" process "<<cnt<<endl;
        }
    }
    cout<<"end thread "<<this_thread::get_id()<<endl;
}

int main(){
    LOG::log_level=LOG::LogLevel::INFO;
    string key,value;
    DB db = DB::create("test_db");
    const int thread_cnt = 8;
    vector<std::thread> threads(thread_cnt);
    string files[8]={"../scripts/xaa",
                     "../scripts/xab",
                     "../scripts/xac",
                     "../scripts/xad",
                     "../scripts/xae",
                     "../scripts/xaf",
                     "../scripts/xag",
                     "../scripts/xah"};
    log_info("start set");
    timer::start("total set time");
    for(int i=0;i<thread_cnt;i++){
        threads[i] = thread(func,ref(db),files[i]);
    }
    for(int i=0;i<thread_cnt;i++){
        threads[i].join();
    }
    timer::end("total set time");
    timer::print();
    return 0;
}