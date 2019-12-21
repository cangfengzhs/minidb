//
// Created by cangfeng on 2019/12/11.
//

#include "timer.h"

 std::map<std::string,std::vector<std::chrono::duration<double>>> timer::durations= std::map<std::string,std::vector<std::chrono::duration<double>>>();
 std::map<std::string,std::chrono::steady_clock::time_point> timer::starts=std::map<std::string,std::chrono::steady_clock::time_point>();