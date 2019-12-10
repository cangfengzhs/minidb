//
// Created by cangfeng on 2019/12/1.
//

#ifndef MINIDB_SKIPLIST_H
#define MINIDB_SKIPLIST_H


#include "format.h"
#include "config.h"
#include "error.h"
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
        };
        //比较器
        std::function<int(const Key &, const Key &)> cmp_;
        vec<ptr<Node>> pre_;

        ptr<Node> _add(ptr<Node> pre, const Key &key, int cur_level, int ins_level) {
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
            ptr<Node> down = _add(pre->down, key, cur_level - 1, ins_level);
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

        SkipList(std::function<int(const Key &, const Key &)> cmp) : cmp_(cmp), pre_(config::SKIPLIST_MAX_LEVEL) {
            for (int i = 0; i < config::SKIPLIST_MAX_LEVEL; i++) {
                pre_[i] = std::make_shared<Node>();
            }
            for (int i = 1; i < config::SKIPLIST_MAX_LEVEL; i++) {
                pre_[i]->down = pre_[i - 1];
            }
        }

        void add(const Key &key) {
            int ins_level = rand() % config::SKIPLIST_MAX_LEVEL;
            ptr<Node> pre = pre_.back();
            _add(pre, key, config::SKIPLIST_MAX_LEVEL - 1, ins_level);
        }

        //查找第一个大于等于key的key
        Key seek(const Key &key) {
            ptr<Node> x = _seek(pre_.back(), key);
            if (x == nullptr) {
                throw KeyNotFound<Key>(key);
            }
            return x->key;
        }
        class Iterator{
            ptr<Node> current;
            friend class SkipList;
            Iterator(ptr<Node> x){
                current=x;
            }
        public:
            bool hash_next(){
                return current!= nullptr&&current->right!= nullptr;
            }
            Key next(){
                current = current->right;
                return current->key;
            }
        };
        Iterator iterator(){
            ptr<Node> x= nullptr;
            if(pre_.size()>0){
                x=pre_.front();
            }
            return Iterator(x);
        }
    };
}


#endif //MINIDB_SKIPLIST_H
