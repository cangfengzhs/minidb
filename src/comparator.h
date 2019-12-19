//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_COMPARATOR_H
#define MINIDB_COMPARATOR_H

#include "slice.h"
#include "format.h"
#include "record.h"
namespace minidb{
    inline int userkey_comparator(const ptr<Slice>& a,const ptr<Slice>& b);
    inline int record_comparator(const ptr<Record>& a,const ptr<Record>& b);
    inline int userkey_comparator(const ptr<Slice>& a,const ptr<Slice>& b){
        int n = a->size();
        int m = b->size();
        if(n<m){
            return -1;
        }
        if(n>m){
            return 1;
        }
        const char* x = a->data();
        const char* y = b->data();
        for(int i=0;i<n;i++){
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

    inline int record_comparator(const ptr<Record>& a,const ptr<Record>& b){
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
#endif //MINIDB_COMPARATOR_H
