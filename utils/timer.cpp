//
// Created by cangfeng on 2019/12/11.
//

#include "timer.h"

 std::map<std::string,std::vector<double>> timer::durations=std::map<std::string,std::vector<double>>();
 std::map<std::string,clock_t> timer::starts=std::map<std::string,clock_t>();