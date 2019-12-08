//
// Created by cangfeng on 2019/12/1.
//
#include <string>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <cassert>
#include <sys/mman.h>
#include "file_util.h"
#include "config.h"

namespace minidb {
    void create_dir(const std::string &dir_name) {
        int result = mkdir(dir_name.c_str(), 0755);
        if (result == -1) {
            //TODO throw exception
        }
    }

    int create_file(const std::string &file_name) {
        int fd = open(file_name.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 0644);
        if (fd == -1) {
            //TODO throw exception
        }
        return fd;
    }

    std::string fn_fmt(int file_number) {
        std::string s(8, '0');
        for (int i = 7; i >= 0; i--) {
            s[i] = file_number % 10 + '0';
            file_number /= 10;
        }
        return std::move(s);
    }

    FileMeta::FileMeta(const std::string &file_name, int file_number, int fd) {
        this->file_name = file_name;
        this->file_number = file_number;
        this->fd = fd;
        remove_flag = false;
    }
    FileMeta::~FileMeta(){
        close(fd);
        if(remove_flag){
            remove(file_name.c_str());
        }
    }
    int FileMeta::remove_file() {
        remove_flag = true;
        return 0;
    }
    BufWriter::BufWriter(const std::string &file_name, bool end_with_magic, bool cover) {
        auto mod = O_WRONLY;
        if (cover) {
            mod |= O_CREAT | O_TRUNC;
        }
        int fd = open(file_name.c_str(),mod,0644);
        filemeta.file_name=file_name;
        filemeta.file_number=-1;
        filemeta.fd=fd;
        buf_offset=0;
        size_=0;
    }
    int BufWriter::remove() {
        return filemeta.remove_file();
    }
    bool BufWriter::sync() {
        fsync(filemeta.fd);
        return true;
    }
    uint64_t BufWriter::size() {
        return size_;
    }
    int BufWriter::write(void * data, int size) {
        return write((const char*)data,size);
    }
    int BufWriter::write(const char * data, int size) {
        for(int i=0;i<size;i++){
            buf[buf_offset++]=data[i];
            if(buf_offset==1024){
                flush();
            }
        }
        return size;
    }
    bool BufWriter::flush() {
        int cnt = write(buf,buf_offset);
        assert(cnt==buf_offset);
        buf_offset=0;
        return true;
    }
    int BufWriter::close() {
        if(buf_offset>0){
            flush();
        }
        sync();
        return 0;
    }
    MmapReader::MmapReader(const std::string &file_name, bool end_with_magic) {
        filemeta.file_name=file_name;
        int fd = open(file_name.c_str(),O_RDONLY);
        uint64_t size = lseek(fd,0,SEEK_END);
        size_=size;
        data=(char*)mmap(nullptr,size,PROT_READ,MAP_SHARED,fd,0);
        if(end_with_magic){
            size_-=8;
            assert(config::MAGIC==*(uint64_t*)(data+size));
        }
        close(fd);
    }
    int MmapReader::size() {
        return size_;
    }
    int MmapReader::remove() {
        return filemeta.remove_file();
    }
    char * MmapReader::base() {
        return data;
    }
    int MmapReader::seek(uint64_t offset) {
        if(offset<size_){
            offset_=offset;
            return 0;
        }
        assert(offset>=size_);
        return -1;
    }
    int MmapReader::read(void * dest, int size) {
        return read((char*)data,size);
    }
    int MmapReader::read(char * dest, int size) {
        int i=0;
        for(i=0;i<size&&offset_<size_;i++){
            dest[i]=this->data[offset_++];
        }
        return i;
    }

}