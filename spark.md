---
title: "金融大数据作业9"
author: "wangke"
date: "2017年12月26日"
output: pdf_document
---

安装spark</br>
------------------
spark的下载，首先从官网下下载，而后进行一系列的配置</br>
首先配置环境变量，将其写入环境变量当中，这样便可以直接输入pyspark进行启动</br>
```{}
#setting path for spark
export SPARK_PATH=~/spark2.2.1
export PATH=$SPARK_PATH/bin:$PATH
export PYTHONPATH=$SPARK_PATH/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_PATH/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
alias snotebook='$SPARK_PATH/bin/pyspark --masterlocal[2]'
```
![安装](https://github.com/WangKe2333/spark/raw/master/picture/安装spark.png)

实现project1的功能</br>
----------------------------
+ 首先进行分词工作，使用Python当中的jieba分词</br>
```{}
def mycut(x,stop):
    seg=jieba.cut(x)
    t=[]
    for word in seg:
        if(word not in stop and word >= u'\u4e00' and word <= u'\u9fa5'):
            t.append(word)
```
+ 分词和停词,读入停词文件</br>
```{}
    #读取停词文件
    f=open("/Users/wangke/Desktop/stopwords.txt",encoding="gbk")
    t=f.readlines()
    for i in range(0,len(t)):
        t[i]=t[i].replace("\n","")
```
+ 随后进行Wordcount,利用spark和Python的简洁性我们用四行便可以完成此事</br>
```{}
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: mycut(x,t)) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
```
+ 而后便是project1的另一个需求---按照词频从高到低输出,在MapReduce当中我们要另外写一个Job完成排序的功能，调换key--value对，而在spark上，因为有封装好的sortbykey用一行便可以完成此事</br>
```{}
#map调换key--value对，sortByKey进行排序，False参数实现倒序
output = counts.map(lambda x:(x[1],x[0])).sortByKey(False).collect()
```

![结果](https://github.com/WangKe2333/spark/raw/master/picture/结果.png)

+ 输入任意k值输出相应结果</br>
```{}
    for (count, word) in output:
        if(count>int(sys.argv[2])):
            print("%s: %i" % (word, count))
            f.write(word+","+str(count)+"\n")
```
![k](https://github.com/WangKe2333/spark/raw/master/picture/k值.png)
+ **另外，为了后面的sql工作，我还实现了一个输出到json格式的程序**</br>
```{}
    for (count, word) in output:
        if(count>int(sys.argv[2])):
            print("%s: %i" % (word, count))
            datas={"word":word,"count":count}
            f.write(json.dumps(datas,ensure_ascii=False,indent=1))
            f.write("\n")
```
+ json格式的输出</br>

![json](https://github.com/WangKe2333/spark/raw/master/picture/json.png)</br>

spark_sql(基于pyspark的实现)
-----------------------------
+ 首先是基本的连接和实现</br>
```{}
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
```
+ 而后是json格式的，更为方便一些
```{}
def myjson(spark):
    
    df = spark.read.json("/Users/wangke/Desktop/output.json")
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
```
+ sql的输出结果</br>
![output](https://github.com/WangKe2333/spark/raw/master/picture/output1.png)</br>
![output](https://github.com/WangKe2333/spark/raw/master/picture/output2.png)</br>
![output](https://github.com/WangKe2333/spark/raw/master/picture/output3.png)</br>
![output](https://github.com/WangKe2333/spark/raw/master/picture/output4.png)</br>
![output](https://github.com/WangKe2333/spark/raw/master/picture/output5.png)</br>
![output](https://github.com/WangKe2333/spark/raw/master/picture/output6.png)</br>
![output](https://github.com/WangKe2333/spark/raw/master/picture/output7.png)</br>


一些感受</br>
-------------------
+ spark比起MapReduce要简洁的多，有一个重要的原因也是Python本身的代码比较简洁 </br> Python的lambda函数也十分好用，在MapReduce中要写一个很长的函数实现的功能，在spark当中使用.map即可实现，spark当中还有很多封装好的.sortByKey等等，都大大提高了代码的抽象水平</br>
另外，与Python的MapReduce相比，Python的spark版本要快很多，同样是使用jieba分词，spark跑的就会快很多</br>
+ pyspark在安装好HDFS,JVM和Python的情况下，基本不需要任何配置便可以运行,若要与HDFS连接也很容易只需要更改相应的输入路径，提前启动HDFS
```{}

bin/spark-submit /Users/wangke/Desktop/mywc2.py hdfs://localhost:9000/user/wangke/fulldata.txt 500

#读取停词文件
    f=open("hdfs://localhost:9000/user/wangke/stopwords.txt",encoding="gbk")
    t=f.readlines()
    for i in range(0,len(t)):
        t[i]=t[i].replace("\n","")
```

pyspark交互界面与localhost:4040
-----------------------------------
+ 如果使用pyspark命令行交互而不是提交py文件的话，可以在localhost:4040看到相应的执行速度</br>
+ spark为第一项工作设计的DAG执行流程图</br>
![DAG](https://github.com/WangKe2333/spark/raw/master/picture/DAG1.png)</br>
+ task的具体执行描述信息</br>
![task](https://github.com/WangKe2333/spark/raw/master/picture/task.png)</br>
+ spark为sort工作设计的DAG执行流程图</br>
![DAG](https://github.com/WangKe2333/spark/raw/master/picture/DAG2.png)</br>
+ task的具体执行描述信息</br>
![DAG](https://github.com/WangKe2333/spark/raw/master/picture/task2.png)</br>



