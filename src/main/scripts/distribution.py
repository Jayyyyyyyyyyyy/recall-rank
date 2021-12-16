#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
import redis

if len(sys.argv) != 2:
    print >> sys.stderr, "usage: %s <updateFile>  > output"  % sys.argv[0]

updateFile = sys.argv[1]

Host = "10.19.170.56"
Port = 6379
extime = 60*60*24*7

pool = redis.ConnectionPool(host=Host, port=Port)
r = redis.Redis(connection_pool=pool)
p = r.pipeline(transaction=True)
count=0
with open(updateFile, 'r') as file:
distribution = []
for line in file(updateFile):
    try:
        line = line.strip("\n")
        words = line.split("\t")
        if len(words) == 2:
            name = words[0]
            value = [idscore.split(":")[0] for idscore in words[1].split(" ")]
            distribution.append(len(value))

#             r.delete(name)
#             r.rpush(name, *value)
#             r.expire(name, extime)
            count+=1
    except:
        pass
#p.execute()
print("imported:"+str(count)+" lines")