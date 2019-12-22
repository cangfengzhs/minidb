//
// Created by cangfeng on 2019/12/1.
//


#include "slice.h"
#include <cstring>
namespace minidb{
    Slice::Slice(const std::string &str) {
        size_ = str.size();
        data_ = new char[size_];
        memcpy(data_,str.data(),size_);
    }
    Slice::Slice(const char *start, const char* end) {
        size_=end-start;
        data_=new char[size_];
        memcpy(data_,start,size_);
    }
    bool Slice::operator==(const minidb::Slice &ref) const {
        if(size_!=ref.size_){
            return false;
        }
        return memcmp(data_,ref.data_,size_)==0;
    }
    Slice::Slice(size_t size) {
        size_=size;
        data_ = new char[size_];
    }
    Slice::~Slice(){
        delete [] data_;
    }
}