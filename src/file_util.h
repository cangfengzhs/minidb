//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_FILE_UTIL_H
#define MINIDB_FILE_UTIL_H

#include <string>

namespace minidb{
    void create_dir(const std::string& dir_name);
    int create_file(const std::string& file_name);
    int get_version_pointer(const std::string& db_name);
    int set_version_pointer(const std::string& db_name,int version_fd);
    std::string fn_fmt(int file_number);
    struct FileMeta{
        std::string file_name;
        int file_number;
        int fd=-1;
        bool remove_flag=false;
        FileMeta()= default;
        FileMeta(const std::string& file_name,int file_number,int fd);
        ~FileMeta();
        int remove_file();
    };

    class BufWriter{
        char buf[8192];
        int buf_offset;
        uint64_t size_;
    public:
        FileMeta filemeta;
        BufWriter(const std::string& file_name,bool end_with_magic,bool cover);
        int append(const char* data,int size);
        int append(void* data,int size);
        bool flush();
        bool sync();
        uint64_t size();
        int remove();
        int close();
    };
    class MmapReader{
        char* data;
        int size_;
        int file_size;
        FileMeta filemeta;
        int offset_;

    public:
        MmapReader(const std::string& file_name,bool end_with_magic);
        int read(char* dest,int size);
        int read(void* dest,int size);
        int remove();
        int seek(uint64_t offset);
        int size();
        int remain();
        char* base();
        ~MmapReader();
    };
}
#endif //MINIDB_FILE_UTIL_H
