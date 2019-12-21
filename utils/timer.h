//
// Created by cangfeng on 2019/12/11.
//

#ifndef MINIDB_TIMER_H
#define MINIDB_TIMER_H

#include <map>
#include <vector>
#include <string>
#include <chrono>
#include <algorithm>
class timer{
    static std::map<std::string,std::vector<std::chrono::duration<double>>> durations;
    static std::map<std::string,std::chrono::steady_clock::time_point> starts;
public:
    static inline void start(const std::string& name){
        starts[name]=std::chrono::steady_clock::now();
    }
    static inline void end(const std::string& name){
        if(starts.count(name)){
            if(!durations.count(name)){
                durations[name]=std::vector<std::chrono::duration<double>>();
            }
            durations[name].push_back(
                    std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now()-starts[name]));
            starts.erase(name);
        }
    }
    static inline void print(){
        for(auto & iter : timer::durations){
            auto name = iter.first;
            auto& duration_list = iter.second;
            int cnt = duration_list.size();
            std::chrono::duration<double> total{};
            for(auto dur:duration_list){
                total+=dur;
            }
            std::chrono::duration<double> avg = 1.0*total/cnt;
            printf("Name: %s\tcount: %d\ttotal time: %.4f\tavg time: %.4f\n",name.c_str(),cnt,total.count(),avg.count());
        }
    }
};
#endif //MINIDB_TIMER_H
