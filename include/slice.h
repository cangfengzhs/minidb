//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_SLICE_H
#define MINIDB_SLICE_H

#include <string>

namespace minidb{

    class Slice{
        char* data_;
        size_t size_;
    public:
        Slice(size_t size);
        explicit Slice(const std::string& str);
        Slice(const char* start,const char* end);
        inline int size(){return size_;}
        inline const char* data(){return data_;}
        bool operator==(const Slice& ref)const;
        ~Slice();
    };
}



#endif //MINIDB_SLICE_H
