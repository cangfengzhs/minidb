//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_FILE_UTIL_H
#define MINIDB_FILE_UTIL_H

#include <string>

namespace minidb::file_util{
    void create_dir(const std::string& dir_name);
    int create_file(const std::string& file_name);
}
#endif //MINIDB_FILE_UTIL_H
