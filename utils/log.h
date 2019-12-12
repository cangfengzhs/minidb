//
// Created by cangfeng on 2019/12/11.
//

#ifndef MINIDB_LOG_H
#define MINIDB_LOG_H

#include <cstdio>
#include <cstdint>
#include <string>
class LOG{
public:
    enum class LogLevel:uint8_t {
        DEBUG,
        INFO,
        WARNING,
        ERROR,
        OFF
    };
    static LogLevel log_level;
    template<typename format,typename ...Args>
    static void info(const std::string& file, const char *func, int line, format fmt, Args ...args){
        if(log_level>LogLevel::INFO){
            return;
        }
        printf("\033[30m%s:%s:%d [INFO]:",file.c_str(),func,line);
        printf(fmt,args...);
        printf("\033[0m\n");
    }
    template<typename format,typename ...Args>
    static void warning(const std::string& file,const char* func,int line,format fmt,Args ...args){
        if(log_level>LogLevel::WARNING){
            return;
        }
        printf("\033[34m%s:%s:%d [WARN]:",file.c_str(),func,line);
        printf(fmt,args...);
        printf("\033[0m\n");
    }
    template<typename format,typename ...Args>
    static void error(const std::string& file,const char* func,int line,format fmt,Args ...args){
        if(log_level>LogLevel::ERROR){
            return;
        }
        printf("\033[31m%s:%s:%d [ERROR]:",file.c_str(),func,line);
        printf(fmt,args...);
        printf("\033[0m\n");
    }
    template<typename format,typename ...Args>
    static void debug(const std::string& file,const char* func,int line,format fmt,Args ...args){
        if(log_level>LogLevel::DEBUG){
            return;
        }
        printf("\033[32m%s:%s:%d [DEBUG]:",file.c_str(),func,line);
        printf(fmt,args...);
        printf("\033[0m\n");
    }
};
#define log_info(...) LOG::info(__FILE__,__FUNCTION__,__LINE__,__VA_ARGS__)
#define log_warn(...) LOG::warning(__FILE__,__FUNCTION__,__LINE__,__VA_ARGS__)
#define log_error(...) LOG::error(__FILE__,__FUNCTION__,__LINE__,__VA_ARGS__)
#define log_debug(...) LOG::debug(__FILE__,__FUNCTION__,__LINE__,__VA_ARGS__)



#endif //MINIDB_LOG_H
