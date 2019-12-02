//
// Created by cangfeng on 2019/12/2.
//

#include "comparator.h"
#include <algorithm>
namespace minidb{
    int userkey_comparator(const ptr<Slice>& a,const ptr<Slice>& b){
        int n = a->size();
        int m = b->size();
        const char* x = a->data();
        const char* y = b->data();
        for(int i=0;i<std::min(n,m);i++){
            if(x[i]<y[i]){
                return -1;
            }
            if(x[i]>y[i]){
                return 1;
            }
        }
        if(n<m){
            return -1;
        }
        if(n>m){
            return 1;
        }
        return 0;
    }

    int record_comparator(const ptr<Record>& a,const ptr<Record>& b){
        int ret = userkey_comparator(a->user_key(),b->user_key());
        if(ret!=0){
            return ret;
        }
        if(a->lsn()>b->lsn()){
            return -1;
        }
        if(a->lsn()<b->lsn()){
            return 1;
        }
        return 0;
    }

}