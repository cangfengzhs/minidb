//
// Created by cangfeng on 2019/12/1.
//
#include <string>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <cassert>
#include <sys/mman.h>
#include "file_util.h"
#include "config.h"
#include "log.h"
#include <unistd.h>
#include <error.h>
#include <cstring>
#include <cstdio>
namespace minidb {
    void create_dir(const std::string &dir_name) {
        int result = mkdir(dir_name.c_str(), 0755);
        if (result == -1) {
            //TODO throw exception
        }
    }
    int get_version_pointer(const std::string& db_name){
        int fd = open((db_name+"/version_pointer").c_str(),O_RDONLY);
        log_debug("[Version pointer] fd: %d",fd);
        if(fd==-1){
            //TODO throw exception
        }
        int version_fn;
        uint64_t magic;
        read(fd,&version_fn,4);
        read(fd,&magic,8);
        assert(magic==config::MAGIC);
        close(fd);
        return version_fn;
    }
    int set_version_pointer(const std::string& db_name,int version_fn){
        int fd=open((db_name+"/version_pointer").c_str(),O_WRONLY|O_CREAT|O_TRUNC,0644);
        log_debug("[Version pointer] fd: %d",fd);
        if(fd==-1){
            //TODO throw exception
        }
        write(fd,&version_fn,4);
        write(fd,&config::MAGIC,8);
        ::fsync(fd);
        close(fd);
        return 0;
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
        log_debug("[close fd] %d",fd);
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
        log_debug("open fd:%d",fd);
        assert(fd!=-1);
        filemeta.file_name=file_name;
        filemeta.file_number=-1;
        filemeta.fd=fd;
        buf_offset=0;
        size_=0;
    }
    int BufWriter::remove() {
        log_debug("[buf writer] remove file %s",filemeta.file_name.c_str());
        return filemeta.remove_file();
    }
    bool BufWriter::sync() {
        fsync(filemeta.fd);
        return true;
    }
    uint64_t BufWriter::size() {
        return size_;
    }
    int BufWriter::append(void * data, int size) {
        return append((const char*)data,size);
    }
    int BufWriter::append(const char * data, int size) {
        int total=0;
        while(size>0){
            int cnt = std::min(size,config::BUFWRITER_BUF_SIZE-buf_offset);
            memcpy(buf+buf_offset,data+total,cnt);
            size-=cnt;
            total+=cnt;
            buf_offset+=cnt;
            if(buf_offset==config::BUFWRITER_BUF_SIZE){
                flush();
            }
        }
        size_+=total;
        return total;
    }
    bool BufWriter::flush() {
        int cnt = write(filemeta.fd,buf,buf_offset);
        if(cnt!=buf_offset){
            log_debug("[BufWriter]error no:%d",errno);
        }
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
        log_debug("open fd:%d",fd);
        filemeta.fd=fd;
        if(fd==-1){
            log_error("open file:%s failed",file_name.c_str());
        }
        file_size = lseek(fd,0,SEEK_END);
        size_=file_size;
        data=(char*)mmap(nullptr,file_size,PROT_READ,MAP_SHARED,fd,0);
        if(end_with_magic){
            size_-=8;
            assert(config::MAGIC==*(uint64_t*)(data+size_));
        }
        offset_=0;
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
        return read((char*)dest,size);
    }
    int MmapReader::read(char * dest, int size) {
        int i=0;
        for(i=0;i<size&&offset_<size_;i++){
            char t = data[offset_++];
            dest[i]= t;
        }
        return i;
    }
    MmapReader::~MmapReader(){
        munmap(base(),file_size);
    }
    int MmapReader::remain() {
        return size_-offset_;
    }
}