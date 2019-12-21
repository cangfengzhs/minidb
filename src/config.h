//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_CONFIG_H
#define MINIDB_CONFIG_H


namespace minidb::config{
    const int SKIPLIST_MAX_LEVEL = 20;
    const int SSTABLE_LEVEL = 5;
    const int SSTABLE_MAX_FILE_COUNT=4;
    const int SSTABLE_SOFT_MAX_FILE_COUNT=8;
    const uint64_t SSTABLE_FILE_SIZE[5]={16*1024*1024,128*1024*1024,1024ll*1024*1024,8192ll*1024*1024,64ll*1024*1024*1024};
    const int MEMTABLE_MAX_SIZE = 16*1024*1024;
    const int BUFWRITER_BUF_SIZE = 32*1024;
    const uint64_t MAGIC=0x0123456789abcdef;
    const int BLOCK_SIZE=64*1024;
    const int MAX_MISS_TIMES = 1024*1024;
}


#endif //MINIDB_CONFIG_H
