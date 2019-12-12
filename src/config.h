//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_CONFIG_H
#define MINIDB_CONFIG_H


namespace minidb::config{
    const int SKIPLIST_MAX_LEVEL = 20;
    const int MEMTABLE_MAX_SIZE = 4*1024*1024;
    const int LOG_BUF_SIZE = 2*1024;
    const uint64_t MAGIC=0x0123456789abcdef;
    const int BLOCK_SIZE=8*1024;
}


#endif //MINIDB_CONFIG_H
