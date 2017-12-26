---
title: "金融大数据作业7"
author: "wangke"
date: "2017年12月11日"
output: pdf_document
---

1.安装hive</br>
hive的下载安装与hbase类似，首先从官网下下载，而后进行一系列的配置</br>
首先配置环境变量，将其写入环境变量当中，这样便可以直接输入hive进行启动</br>
```{}
#setting path for hive
export HIVE_HOME=/Users/wangke/Downloads/apache-hive-1.2.2-bin
export PATH=$HIVE_HOME/bin:$PATH

#setting hadoop
export HADOOP_HOME=/Users/wangke/hadoop_installs/hadoop-2.7.4
```
而后，因为配置文件都是.template将文件都拷贝一份出来，去掉.template后缀名</br>
```{}
cp hive-env.sh.template hive-env.sh
cp hive-default.xml.template hive-site.xml
cp hive-default.xml.template hive-default.xml
cp hive-log4j.properties.template hive-log4j.properties
cp hive-exec-log4j.properties.template hive-exec-log4j.properties
```
default文件可能要拷贝两次,一个作为全局，一个作为用户设置部分</br>
接下来要更改一些环境变量，主要目的是将Hadoop与hive连接起来，Hadoop要授权给hive允许hive进行访问</br>
HADOOP_HOME和hdfs下为hive创建的文件夹要设置好，否则就会报连接错误</br>
```{}
Exception in thread "main" java.lang.RuntimeException: java.net.ConnectException: Call From wangkedemacbook-air.local/172.28.129.183 to localhost:9000 failed on connection exception: java.net.ConnectException: Connection refused; For more details see:  http://wiki.apache.org/hadoop/ConnectionRefused
	at org.apache.hadoop.hive.ql.session.SessionState.start(SessionState.java:522)
	at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:677)
	at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:621)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.hadoop.util.RunJar.run(RunJar.java:221)
	at org.apache.hadoop.util.RunJar.main(RunJar.java:136)
Caused by: java.net.ConnectException: Call From wangkedemacbook-air.local/172.28.129.183 to localhost:9000 failed on connection exception: java.net.ConnectException: Connection refused; For more details see:  http://wiki.apache.org/hadoop/ConnectionRefused
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
```

修改hive-env.sh，添加JAVA_HOME,HADOOP_HOME</br>
```{}
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home
export HADOOP_HOME=/Users/wangke/hadoop_installs/hadoop-2.7.4
export HIVE_HOME=/Users/wangke/Downloads/apache-hive-1.2.2-bin
export HIVE_CONF_DIR=/Users/wangke/Downloads/apache-hive-1.2.2-bin/conf
```

在hdfs当中为hive创建文件夹并授权</br>
```{}
bin/hdfs dfs -mkdir -p /user/hive/warehouse
bin/hdfs dfs -mkdir -p /user/hive/tmp
bin/hdfs dfs -mkdir -p /user/hive/log
bin/hdfs dfs -chmod -R 777 /user/hive/warehouse
bin/hdfs dfs -chmod -R 777 /user/hive/tmp
bin/hdfs dfs -chmod -R 777 /user/hive/log
```
hive-site.xml中对应要改成上面授权过hive的文件夹</br>
```{}
<property>
    <name>hive.exec.scratchdir</name>
    <value>/user/hive/tmp</value>
</property>
<property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
</property>
<property>
    <name>hive.querylog.location</name>
    <value>/user/hive/log</value>
</property>
```

2.成功启动hive之后进行一些QL操作</br>
![建表](https://github.com/WangKe2333/Homework7/raw/master/picture/建表.png)

</br>输入任意k值输出相应结果</br>
![查询](https://github.com/WangKe2333/Homework7/raw/master/picture/2.png)
</br>k=3000</br>
![k=3000](https://github.com/WangKe2333/Homework7/raw/master/picture/k=3000.png)

