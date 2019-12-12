//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_SKIPLIST_H
#define MINIDB_SKIPLIST_H


#include "format.h"
#include "config.h"
#include "error.h"
#include "timer.h"
#include <cstdlib>
#include <cassert>
#include <functional>
#include <iostream>

namespace minidb {
    /**
     * SkipList是一个只支持插入和查找的跳表（线程不安全）
     * Key:数据类型
     */
    template<typename Key>
    class SkipList {
        struct Node {
            Key key;
            ptr<Node> right = nullptr;
            ptr<Node> down = nullptr;

            ~Node() {
                right = nullptr;
                down = nullptr;
            }
        };

        //比较器
        std::function<int(const Key &, const Key &)> cmp_;
        vec<ptr<Node>> pre_;
        vec<int> level_add_cnt;

        ptr<Node> _add(ptr<Node> pre, const Key &key, int cur_level, int ins_level, int &cnt) {
            cnt++;
            if (cur_level < 0) {
                return nullptr;
            }
            ptr<Node> x = pre->right;
            int c = -1;
            while (x) {
                c = cmp_(x->key, key);
                if (c < 0) {
                    pre = x;
                    x = pre->right;
                    continue;
                } else {
                    break;
                }
            }
            assert(c != 0);
            ptr<Node> down = _add(pre->down, key, cur_level - 1, ins_level, cnt);
            if (cur_level <= ins_level) {
                ptr<Node> ret = std::make_shared<Node>();
                pre->right = ret;
                ret->down = down;
                ret->key = key;
                ret->right = x;
                return ret;
            } else {
                return nullptr;
            }
        }

        ptr<Node> _seek(ptr<Node> pre, const Key &key) {
            ptr<Node> x = pre->right;
            int c = -1;
            while (x) {
                c = cmp_(x->key, key);
                if (c < 0) {
                    pre = x;
                    x = pre->right;
                    continue;
                } else {
                    break;
                }
            }
            if (pre->down) {
                return _seek(pre->down, key);
            } else {
                return x;
            }
        }

    public:
        SkipList() = delete;

        SkipList(std::function<int(const Key &, const Key &)> cmp) : cmp_(cmp), pre_(config::SKIPLIST_MAX_LEVEL),
                                                                     level_add_cnt(config::SKIPLIST_MAX_LEVEL, 0) {
            for (int i = 0; i < config::SKIPLIST_MAX_LEVEL; i++) {
                pre_[i] = std::make_shared<Node>();

            }
            for (int i = 1; i < config::SKIPLIST_MAX_LEVEL; i++) {
                pre_[i]->down = pre_[i - 1];
            }

        }

        //防止析构递归爆栈
        ~SkipList() {
            vec<ptr<Node>> node_list;
            for (int i = 0; i < pre_.size(); i++) {
                ptr<Node> pre = pre_[i];
                while (pre) {
                    node_list.push_back(pre->right);
                    auto t = pre->right;
                    pre->right = nullptr;
                    pre = t;
                }
            }
            for (int i = 0; i < node_list.size(); i++) {
                node_list[i] = nullptr;
            }
        }

        int next_add_level() {
            int ret=0;
            for(int i=0;i<config::SKIPLIST_MAX_LEVEL;i++){
                if(level_add_cnt[i]>=2){
                    ret=i+1;
                    level_add_cnt[i]=0;
                }else{
                    level_add_cnt[i]++;
                    break;
                }
            }
            ret=ret==config::SKIPLIST_MAX_LEVEL?ret-1:ret;
            return ret;
        }

        void add(const Key &key) {
            int ins_level = next_add_level();
            ptr<Node> pre = pre_.back();
            int cnt;
            timer::start("skiplist real add");
            _add(pre, key, config::SKIPLIST_MAX_LEVEL - 1, ins_level, cnt);
            timer::end("skiplist real add");
        }

        //查找第一个大于等于key的key
        Key seek(const Key &key) {
            ptr<Node> x = _seek(pre_.back(), key);
            if (x == nullptr) {
                throw KeyNotFound<Key>(key);
            }
            return x->key;
        }

        class Iterator {
            ptr<Node> current;

            friend class SkipList;

            Iterator(ptr<Node> x) {
                current = x;
            }

        public:
            bool hash_next() {
                return current != nullptr && current->right != nullptr;
            }

            Key next() {
                current = current->right;
                return current->key;
            }
        };

        Iterator iterator() {
            ptr<Node> x = nullptr;
            if (pre_.size() > 0) {
                x = pre_.front();
            }
            return Iterator(x);
        }
    };
}


#endif //MINIDB_SKIPLIST_H
