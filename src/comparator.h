//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_COMPARATOR_H
#define MINIDB_COMPARATOR_H

#include "slice.h"
#include "format.h"
#include "record.h"
namespace minidb{
    int userkey_comparator(const ptr<Slice>& a,const ptr<Slice>& b);
    int record_comparator(const ptr<Record>& a,const ptr<Record>& b);
}
#endif //MINIDB_COMPARATOR_H
