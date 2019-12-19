//
// Created by cangfeng on 2019/12/4.
//

#include "version.h"
#include <unistd.h>
#include <cassert>
#include <set>
#include <utility>
#include <algorithm>
#include "file_util.h"
#include "config.h"
#include "log.h"
namespace minidb {
    using std::max;
    void Version::remove() {
        filemeta.remove_file();
    }
    Version::Version(minidb::ptr<minidb::LogWriter> log, minidb::ptr<minidb::LogWriter> imm_log,
                     SstSetList& sst_set_list, LogSeqNumber lsn, const std::string &db_name,
                     int file_number, bool create):
                     sst_set_list_(std::move(sst_set_list)),
                     lsn_(lsn),
                     log_(std::move(log)),
                     pre_log_(std::move(imm_log)){
        filemeta.file_name=db_name + "/" + fn_fmt(file_number) + ".ver";
        if (create) {
            BufWriter writer(db_name + "/" + fn_fmt(file_number) + ".ver", true, true);
            int x = log_ ? log_->file_number() : -1;
            writer.append(&x, 4);
            log_debug("[new version] log:%d ",x);
            x = pre_log_ ? pre_log_->file_number() : -1;
            writer.append(&x, 4);
            log_debug("[new version] pre log:%d",x);
            x=0;
            for(const auto& sst_set:sst_set_list_){
                x+=sst_set.size();
            }
            writer.append(&x, 4);
            log_debug("[new version] sst cnt:%d",x);
            for(int i=0;i<sst_set_list_.size();i++){
                for(const auto& sst:sst_set_list_[i]){
                    x = sst->file_number();
                    log_debug("new version sst number:%d",x);
                    writer.append(&x, 4);
                    char level=(char)i;
                    writer.append(&level, level);
                }
            }
            writer.append(&lsn_, sizeof(LogSeqNumber));
            writer.append(reinterpret_cast<const char *>(&config::MAGIC), 8);
            writer.flush();
            writer.sync();
            writer.close();
        }
    }

    ptr<class minidb::Version>
    Version::apply(ptr<class minidb::VersionEdit> edit, const std::string &db_name, int file_number) {
        LogSeqNumber lsn = this->lsn_;
        ptr<LogWriter> log = edit->log_flag?edit->log_:log_;
        ptr<LogWriter> pre_log = edit->pre_log_flag?edit->pre_log_:pre_log_;
        if (log) {
            lsn = max(lsn, log->max_lsn());
        }
        if (pre_log) {
            lsn = max(lsn, pre_log->max_lsn());
        }
        SstSetList sst_set_list;
        for(int i=0;i<sst_set_list.size();i++){
            sst_set_list[i] = sst_set_list_[i];
            for(const auto& rm_sst:edit->remove_sst_[i]){
                sst_set_list[i].erase(rm_sst);
                rm_sst->remove();
            }
        }
        for(int i=0;i<sst_set_list.size();i++){
            sst_set_list[i].insert(edit->add_sst_[i].begin(),edit->add_sst_[i].end());
        }
        ptr<Version> new_version = make_ptr<Version>(log, pre_log, sst_set_list, lsn, db_name, file_number, true);
        return new_version;
    }

    void Version::print() {
        printf("============================================================\n");
        printf("Version: %s\n",filemeta.file_name.c_str());
        if(log_)printf("LOG: %d\n",log_->file_number());
        else printf("LOG: nullptr\n");
        if(pre_log_)printf("PRE LOG: %d\n",pre_log_->file_number());
        else printf("PRE LOG: nullptr\n");
        for(int i=0;i<sst_set_list_.size();i++){
            printf("SST LEVEL %d:",i);
            for(auto sst:sst_set_list_[i]){
                printf("%d\t",sst->file_number());
            }
            printf("\n");
        }
        printf("============================================================\n");
    }

    void VersionEdit::add_sst(const ptr<class minidb::SSTable>& sst, int level) {
        add_sst_[level].insert(sst);
    }
    void VersionEdit::set_log(ptr<class minidb::LogWriter> log) {
        log_flag=true;
        log_=std::move(log);
    }
    void VersionEdit::set_pre_log(ptr<class minidb::LogWriter> log) {
        pre_log_flag=true;
        pre_log_=std::move(log);
    }
    void VersionEdit::remove_sst(const ptr<class minidb::SSTable>& sst, int level) {
        remove_sst_[level].insert(sst);
    }
}