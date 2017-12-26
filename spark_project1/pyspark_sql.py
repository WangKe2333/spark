#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 27 00:58:27 2017

@author: wangke
"""

from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import Row

from pyspark.sql.types import *


def myjson(spark):
    
    df = spark.read.json("/Users/wangke/Desktop/output5.json")
    df.show()
    df.printSchema()
    df.select("word").show()
    df.select(df['word'], df['count'] + 1).show()
    df.filter(df['count'] > 5000).show()
    df.groupBy("count").count().show()
    df.createOrReplaceTempView("wordcount")
    sqlDF = spark.sql("SELECT * FROM wordcount")
    sqlDF.show()
    df.createGlobalTempView("wordcount")
    spark.sql("SELECT * FROM global_temp.wordcount").show()
    spark.newSession().sql("SELECT * FROM global_temp.wordcount").show()


def mysql(spark):
    sc = spark.sparkContext
    lines = sc.textFile("/Users/wangke/Desktop/output.txt")
    parts = lines.map(lambda l: l.split(","))
    wordcount = parts.map(lambda p: Row(word=p[0], count=int(p[1])))
    schemaWord = spark.createDataFrame(wordcount)
    schemaWord.createOrReplaceTempView("wordcount")
    highfreq = spark.sql("SELECT word FROM wordcount WHERE count>=5000")
    highfreq.show()

    highfreqword = highfreq.rdd.map(lambda p: "word: " + p.word).collect()
    for word in highfreqword:
        print(word)


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PySpark SQL") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    myjson(spark)
    mysql(spark)

    spark.stop()