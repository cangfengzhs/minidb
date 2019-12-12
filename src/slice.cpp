//
// Created by cangfeng on 2019/12/1.
//


#include "slice.h"

namespace minidb{
    Slice::Slice(const std::string &str) {
        data_=str;
    }
    Slice::Slice(const char *start, const char* end) {
        data_=std::string(start,end);
    }
    int Slice::size() {
        return data_.size();
    }
    const char* Slice::data() {
        return data_.data();
    }
    bool Slice::operator==(const minidb::Slice &ref) const {
        return data_==ref.data_;
    }

    Slice::Slice(int size):data_(size,0) {}
}