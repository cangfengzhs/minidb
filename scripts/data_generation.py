#coding:utf-8

import random

max_key_len = 500
max_val_len = 1000
char_list="1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


key_set = set()
key_count = 10000
data_count = 1000000

delete_pct = 10
print("gen key set")
for _ in range(key_count):
    l = random.randint(1,max_key_len)
    t = "".join([random.choice(char_list) for _ in range(l)])
    key_set.add(t)
f = open("input2.txt","w")
kv_map = {}
key_list = list(key_set)
print("gen value")
for i in range(data_count):
    key = random.choice(key_list)
    if i%10000==0:
        print(i)
    if random.randint(1,100)<delete_pct:
        kv_map[key]="delete"
        f.write("%s\tdelete\n"%key)
    else:
        l = random.randint(1,max_val_len)
        value = "".join([random.choice(char_list) for _ in range(l)])
        kv_map[key]=value
        f.write("%s\t%s\n"%(key,value))

f.close()
f = open("output2.txt","w")
for key,value in kv_map.items():
    f.write("%s\t%s\n"%(key,value))
f.close()

