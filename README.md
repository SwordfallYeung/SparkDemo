#Spark简要知识：<br/>
spark程序：App<br/>
用于提交应用程序的：Driver<br/>
资源管理：Master<br/>
节点管理：Worker<br/>
执行真正的业务逻辑：Executor<br/>

Executor位于Worker上<br/>

val rdd = sc.textFile("hdfs://node-1.itcast.cn:9000/wc").flatMap(_ .split(" ")).map((_ , 1)).reduceByKey(_ +_ ) <br/>
rdd.toDebugString：可以把rdd操作的依赖关系打印出来<br/>
rdd.dependencies是一个shuffleDependency

远程debug的两种方式：<br/>
第一种方式，远程提交并debug：<br/>
 val conf = new SparkConf().setAppName("WC")<br/>
      .setJars(Array("C:\\HelloSpark\\target\\hello-spark-1.0.jar"))<br/>
      .setMaster("spark://node-1.itcast.cn:7077")<br/>
      
## RDD缓存
①没有采用缓存机制<br/>
val rdd = sc.textFile("hdfs://node-1.itcast.cn:9000/itcast")<br/>
rdd.count<br/>
计算rdd的数量会很慢<br/>
②采用缓存机制<br/>
val rdd = sc.textFile("hdfs://node-1.itcast.cn:9000/itcast").cache()<br/>
rdd.count<br/>
第一次计算rdd的数量会比较慢，以后就很快<br/>
rdd.map(_.split("\t")).map(x => (x(1), 1)).reduceByKey( _ + _ ).collect 
rdd.unpersist(true) 释放内存<br/>

##Checkpoint
①没有缓存 <br/>
sc.setCheckpointDir("hdfs://node-1.itcast.cn:9000/ck20160519")<br/>
val rdd = textFile("hdfs://node-1.itcast.cn:9000/itcast")<br/>
rdd.checkpoint  checkpoint为transformation<br/>
rdd.count<br/>
以上会运行两个job，一个是count计算；一个是checkpoint,把数据结果保存到hdfs上<br/>
②缓存<br/>
val rdd = textFile("hdfs://node-1.itcast.cn:9000/itcast")<br/>
val rdd2 = rdd.map(_ .split("\t")).map(x => (x(1), 1)).reduceByKey(_ + _)<br/>
rdd2.cache()<br/>
rdd2.checkpoint<br/>
rdd2.collect<br/>
