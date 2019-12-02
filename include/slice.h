//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_SLICE_H
#define MINIDB_SLICE_H

#include <string>

namespace minidb{

    class Slice{
        std::string data_;
    public:
        Slice()= default;
        explicit Slice(const std::string& str);
        Slice(const char* start,const char* end);
        int size();
        const char* data();
    };
}



#endif //MINIDB_SLICE_H
