//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_FILE_UTIL_H
#define MINIDB_FILE_UTIL_H

#include <string>

namespace minidb{
    void create_dir(const std::string& dir_name);
    int create_file(const std::string& file_name);
    std::string fn_fmt(int file_number);
    struct FileMeta{
        std::string file_name;
        int file_number;
        int fd;
        bool remove_flag;
        FileMeta(const std::string& file_name,int file_number,int fd);
        ~FileMeta();
        int remove();
    };

    class BufWriter{
        char buf[1024];
        int buf_offset;
        FileMeta filemeta;
        uint64_t size_;
    public:
        BufWriter(const std::string& file_name,bool end_with_magic,bool cover);
        int write(const char* data,int size);
        int write(void* data,int size);
        bool flush();
        bool sync();
        uint64_t size();
        int remove();
        int close();
    };
    class MmapReader{
        char* data;
        int size_;
        FileMeta filemeta;
        int offset_;

    public:
        MmapReader(const std::string& file_name,bool end_with_magic);
        int read(const char* data,int size);
        int read(void* data,int size);
        int remove();
        int seek(uint64_t offset);
        int size();
        char* base();

    };
}
#endif //MINIDB_FILE_UTIL_H
