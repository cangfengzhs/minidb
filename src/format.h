//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_FORMAT_H
#define MINIDB_FORMAT_H

#include <memory>
#include <vector>
#include <functional>
namespace minidb{
    template <typename T> using ptr = std::shared_ptr<T>;
    template <typename T> using vec = std::vector<T>;
    using LogSeqNumber = uint64_t;
    enum class KeyType:unsigned char{
        INSERT,
        DELETE,
        OFFSET,
        LOOKUP
    };
    using Checksum = std::uint32_t;
    template <typename T,typename ...Args>
    inline ptr<T> make_ptr(Args ...args){
        return std::make_shared<T>(args...);
    }

}
#endif //MINIDB_FORMAT_H
