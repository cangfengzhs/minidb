//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_ERROR_H
#define MINIDB_ERROR_H

#include <stdexcept>

namespace minidb {
    template<typename Key>
    class KeyNotFound : public std::exception {
        const Key key_;
    public:
        KeyNotFound(const Key &key) : key_(key) {};
    };
}
#endif //MINIDB_ERROR_H
