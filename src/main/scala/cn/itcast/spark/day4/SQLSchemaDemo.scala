package cn.itcast.spark.day4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * @author y15079
  * @create 2018-05-12 2:12
  * @desc
  **/
object SQLSchemaDemo {
  def main(args: Array[String]): Unit = {
    //本地运行
    val conf = new SparkConf().setAppName("SQLSchemaDemo").setMaster("local[2]")
    //val conf = new SparkConf().setAppName("SQLDemo") 要打包到spark集群上运行则不需要后面的setMaster("local[2]")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    //创建SQLContext
    val sqlContext = new SQLContext(sc)

    //提交到spark集群上运行，需要设置用户，否则无权限执行，本地运行则无需
    //System.setProperty("user.name", "bigdata")

    //集群hdfs路径  hdfs://node-1.itcast.cn:9000/person.txt
    //下面由于是本地运行，所以采用本地路径
    //从指定的地址创建RDD
    val personRDD = sc.textFile("D:\\IDEA\\HelloSpark\\src\\main\\files\\day4\\person.txt").map(_.split(","))
    //通过StructType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )

    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p=>Row(p(0).toInt, p(1).trim, p(2).toInt))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    //注册表
    personDataFrame.registerTempTable("person")
    //执行SQL
    val df = sqlContext.sql("select * from person where age >= 20 order by age desc limit 2").show()

    sc.stop()
  }
}
