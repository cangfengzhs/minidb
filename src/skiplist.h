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
        //Node相关
        struct Node {
            Key key;
            Node* right = nullptr;
            Node* down = nullptr;
        };
        vec<Node*> node_pool_list;
        Node* node_pool= nullptr;
        int node_pool_head=0;
        Node* alloc_node(){
            if(node_pool== nullptr||node_pool_head==1024){
                node_pool=new Node[1024];
                node_pool_list.push_back(node_pool);
                node_pool_head=0;
            }
            Node* ret = &node_pool[node_pool_head++];
            return ret;
        }
        //比较器
        std::function<int(const Key &, const Key &)> cmp_;
        vec<Node*> pre_;

        //获取一个插入level
        unsigned int bits=0;
        inline unsigned int next_add_level() {
            bits++;
            unsigned int ret=0;
            while(ret<config::SKIPLIST_MAX_LEVEL&&((bits>>ret)&1)){
                ret++;
            }
            if(ret==config::SKIPLIST_MAX_LEVEL){
                ret--;
                bits=0;
            }
            return ret;
        }
        Node* _add(Node* pre, const Key &key, int cur_level, int ins_level) {
            if (cur_level < 0) {
                return nullptr;
            }
            Node* x = pre->right;
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
            Node* down = _add(pre->down, key, cur_level - 1, ins_level);
            if (cur_level <= ins_level) {
                Node* ret = alloc_node();
                pre->right = ret;
                ret->down = down;
                ret->key = key;
                ret->right = x;
                return ret;
            } else {
                return nullptr;
            }
        }

        Node* _seek(Node* pre, const Key &key) {
            Node* x = pre->right;
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

        SkipList(std::function<int(const Key &, const Key &)> cmp) : cmp_(cmp), pre_(config::SKIPLIST_MAX_LEVEL){
            for (int i = 0; i < config::SKIPLIST_MAX_LEVEL; i++) {
                pre_[i] = alloc_node();

            }
            for (int i = 1; i < config::SKIPLIST_MAX_LEVEL; i++) {
                pre_[i]->down = pre_[i - 1];
            }

        }

        //防止析构递归爆栈
        ~SkipList() {
            for(Node* np:node_pool_list){
                delete [] np;
            }
        }



        void add(const Key &key) {
            //timer::start("nxt add level");
            int ins_level = next_add_level();
            //timer::end("nxt add level");

            Node* pre = pre_.back();
            //timer::start("skiplist real add");
            _add(pre, key, config::SKIPLIST_MAX_LEVEL - 1, ins_level);
            //timer::end("skiplist real add");
        }

        //查找第一个大于等于key的key
        Key seek(const Key &key) {
            Node* x = _seek(pre_.back(), key);
            if (x == nullptr) {
                throw KeyNotFound<Key>(key);
            }
            return x->key;
        }

        class Iterator {
            Node* current;

            friend class SkipList;

            Iterator(Node* x) {
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
            Node* x = nullptr;
            if (pre_.size() > 0) {
                x = pre_.front();
            }
            return Iterator(x);
        }
    };
}


#endif //MINIDB_SKIPLIST_H
