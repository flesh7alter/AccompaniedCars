### 环境配置

JDK8
Scala 2.11
Spark 2.44
Hadoop 2.8.5


### 打包与提交到spark执行的方法

1.在IDEA maven中打包成jar包

2.上传到服务器sparkSQL

3.执行
```

/usr/local/spark/bin/spark-submit --class "carSQL" --executor-cores 5 --num-executors 4 --executor-memory 14G --driver-memory 16G sparksql.jar data/31.csv


