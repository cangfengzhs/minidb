//
// Created by cangfeng on 2019/12/11.
//

#ifndef MINIDB_TIMER_H
#define MINIDB_TIMER_H

#include <map>
#include <vector>
#include <string>
#include <ctime>

class timer{
    static std::map<std::string,std::vector<double>> durations;
    static std::map<std::string,clock_t> starts;
public:
    static void start(std::string name){
        starts[name]=clock();
    }
    static void end(std::string name){
        if(starts.count(name)){
            if(!durations.count(name)){
                durations[name]=std::vector<double>();
            }
            durations[name].push_back(double(clock()-starts[name])/CLOCKS_PER_SEC);
            starts.erase(name);
        }
    }
    static void print(){
        for(auto iter=timer::durations.begin();iter!=timer::durations.end();iter++){
            auto name = iter->first;
            auto& duration_list = iter->second;
            auto cnt = duration_list.size();
            double total = 0;
            for(auto dur:duration_list){
                total+=dur;
            }
            double avg = 1.0*total/cnt;
            printf("Name: %s\tcount: %d\ttotal time: %.4f\tavg time: %.4f\n",name.c_str(),cnt,total,avg);
        }
    }
};
#endif //MINIDB_TIMER_H
