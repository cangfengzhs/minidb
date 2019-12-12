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
                     minidb::vec<minidb::ptr<minidb::SSTable>> sst_list, LogSeqNumber lsn, const std::string &db_name,
                     int file_number, bool create) {
        filemeta.file_name=db_name + "/" + fn_fmt(file_number) + ".ver";
        lsn_ = lsn;
        log_ = std::move(log);
        pre_log_ = std::move(imm_log);
        sst_list_ = std::move(sst_list);
        file_number = file_number;
        if (create) {
            BufWriter writer(db_name + "/" + fn_fmt(file_number) + ".ver", true, true);
            int x = log_ ? log_->file_number() : -1;
            writer.append(&x, 4);
            log_debug("new version log:%d",x);
            x = pre_log_ ? pre_log_->file_number() : -1;
            writer.append(&x, 4);
            x = sst_list_.size();
            writer.append(&x, 4);
            log_debug("new version sst cnt:%d",x);
            for (int i = 0; i < sst_list_.size(); i++) {
                x = sst_list_[i]->file_number();
                log_debug("new version sst number:%d",x);
                writer.append(&x, 4);
                writer.append(&x, 1);//level标记 备用
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
        ptr<LogWriter> log = edit->log_;
        ptr<LogWriter> pre_log = edit->pre_log_;
        if (log) {
            lsn = max(lsn, log->max_lsn());
        }
        if (pre_log) {
            lsn = max(lsn, pre_log->max_lsn());
        }
        vec<ptr<SSTable>> sst_list = this->sst_list_;
        for (auto s:edit->remove_sst_) {
            for (auto iter = sst_list.begin(); iter != sst_list.end();) {
                if (s == *iter) {
                    iter = sst_list.erase(iter);
                } else {
                    iter++;
                }
            }
        }
        for (auto s:edit->add_sst_) {
            sst_list.push_back(s);
        }
        ptr<Version> new_version = make_ptr<Version>(log, pre_log, sst_list, lsn, db_name, file_number, true);
        return new_version;
    }
    void VersionEdit::add_sst(ptr<class minidb::SSTable> sst, int level) {
        add_sst_.push_back(sst);
    }
    void VersionEdit::set_log(ptr<class minidb::LogWriter> log) {
        printf("%p",log_.get());
        log_=log;
    }
    void VersionEdit::set_pre_log(ptr<class minidb::LogWriter> log) {
        pre_log_=log;
    }
}