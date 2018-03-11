# Spark简要知识：
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

# Checkpoint
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

# Spark提交任务的流程
RDD Objects =>DAGScheduler =>TaskScheduler =>Worker

# RDD的依赖关系
RDD和它依赖的父RDD(s)的关系有两种不同的类型，即窄依赖和宽依赖:<br/>
* 窄依赖指的是每一个父RDD的Partition最多被子RDD的一个Partition使用<br/>
  总结：窄依赖我们形象的比喻为独生子女<br/>
* 宽依赖指的是多个子RDD的Partition会依赖同一个父RDD的Partition<br/>
  总结：宽依赖我们形象的比喻为超生<br/>
Spark的state依赖划分是根据窄依赖和宽依赖的<br/>

# DataFrames 结构化流，类似数据库表
DataFrames与RDD区别：DataFrame多了数据的结构信息，即schema，Spark可以清楚地知道该数据集中包含哪些列、每列的名称和类型各是什么，DataFrame是分布式的Row对象的集合。RDD是分布式的 Java对象的集合，从而导致spark框架不了解该Java对象的内部结构。

DataSet与RDD区别：DataSet的数据是以编码的二进制形式被存储，不需要反序列化就可以执行sorting、shuffle等操作。DataSet创建需要一个显式的Encoder，把对象序列化为二进制，可以把对象的scheme映射为SparkSQL类型，然而RDD依赖于运行时反射机制。

DataFrame与DataSet区别：DataSet可以认为是DataFrame的一个特例，主要区别是DataSet每一个record存储的是一个强类型值，而不是一个Row。DataSet可以在编译时检查类型，并且是面向对象的编程接口，DataFrame是面向Spark SQL的接口。DataFrame和DataSet可以相互转化， df.as[ElementType] 这样可以把DataFrame转化为DataSet， ds.toDF() 这样可以把DataSet转化为DataFrame。

RDD转为DataFrame：<br/>
val rdd = sc.textFile("hdfs://node-1.itcast.cn:9000/person.txt").map(_.split("."))<br/>
case class Person(id:Long, name:String, age:Int)<br/>
val personRDD = rdd.map(x => Person(x(0).toLong, x(1), x(2).toInt))<br/>
val df = personRDD.toDF<br/>
df.show() //show是一个action <br/>

DSL风格的写法<br/>
df.select("id","name").show <br/>
df.select(col("id"),col("name"),col("age")+1).show  <br/>
df.select(col("age") >= 18).show  <br/>
df.groupBy("age").count().show() <br/>

SQL风格的写法<br/>
需要先把DataFrame注册成表：df.registerTempTable("t_person")<br/>
sqlContext.sql("select * from t_persion order by age desc limit 2").show <br/>

以json的格式保存在hdfs上<br/>
personDF.select("id","name").write.json("hdfs://node-1.itcast.cn:9000/json")<br/>

不同于write的保存方式：<br/>
personDF.select("id","name").save("hdfs://node-1.itcast.cn:9000/out000) //保存的格式是parquet

加载在hdfs上以json格式保存的数据：<br/>
val df = sqlContext.load("hdfs://node-1.itcast.cn:9000/json","json")  //加载就是DataFrame格式



