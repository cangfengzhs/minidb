//
// Created by cangfeng on 2019/12/1.
//
#include <string>
#include <sys/stat.h>
#include <errno.h>
#include <sys/fcntl.h>
#include "file_util.h"
namespace minidb{
    void create_dir(const std::string& dir_name){
        int result = mkdir(dir_name.c_str(),0755);
        if(result==-1){
            //TODO throw exception
        }
    }
    int create_file(const std::string& file_name){
        int fd = open(file_name.c_str(),O_WRONLY|O_TRUNC|O_CREAT,0644);
        if(fd==-1){
            //TODO throw exception
        }
        return fd;
    }
}