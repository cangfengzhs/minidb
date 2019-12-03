//
// Created by cangfeng on 2019/12/2.
//

#include "db.h"
#include "slice.h"
#include <sstream>
#include <cassert>
#include <iostream>
using namespace std;
using namespace minidb;

int main(){
    DB db = DB::create("test_db");
    cout<<"start set 1\n";
    for(int i=0;i<100;i++){
        string key = to_string(i);
        string value = to_string(i*2);
        db.set(make_shared<Slice>(key),make_shared<Slice>(value));
    }
    cout<<"start set 2\n";
    for(int i=0;i<100;i+=2){
        string key = std::to_string(i);
        string value = std::to_string(i*4);
        db.set(make_shared<Slice>(key),make_shared<Slice>(value));
    }
    cout<<"start delete\n";
    for(int i=0;i<100;i+=4){
        cout<<"delete "<<i<<endl;
        string key = std::to_string(i);
        db.remove(make_shared<Slice>(key));
    }
    cout << "start get\n";
    for(int i=0;i<100;i++){
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