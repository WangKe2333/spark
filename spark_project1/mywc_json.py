#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 26 22:14:40 2017

@author: wangke
"""

from __future__ import print_function

import sys
import jieba
import json
from operator import add

from pyspark.sql import SparkSession



#进行中文分词
def mycut(x,stop):
    seg=jieba.cut(x)
    t=[]
    for word in seg:
        if(word not in stop and word >= u'\u4e00' and word <= u'\u9fa5'):
            t.append(word)
    return t

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(sys.argv[0])
        print("Usage: wordcount <file> k", file=sys.stderr)
        exit(-1)

    #读取停词文件
    f=open("/Users/wangke/Desktop/stopwords.txt",encoding="gbk")
    t=f.readlines()
    for i in range(0,len(t)):
        t[i]=t[i].replace("\n","")
        
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: mycut(x,t)) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.map(lambda x:(x[1],x[0])).sortByKey(False).collect()
    f=open("/Users/wangke/Desktop/output5.json",'a')
    for (count, word) in output:
        if(count>int(sys.argv[2])):
            print("%s: %i" % (word, count))
            datas={"word":word,"count":count}
            f.write(json.dumps(datas,ensure_ascii=False))
            f.write("\n")

    spark.stop()