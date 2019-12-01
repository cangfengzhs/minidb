//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_FORMAT_H
#define MINIDB_FORMAT_H

#include <memory>
#include <vector>
namespace minidb{
    template <typename T> using ptr = std::shared_ptr<T>;
    template <typename T> using vec = std::vector<T>;
}
#endif //MINIDB_FORMAT_H
