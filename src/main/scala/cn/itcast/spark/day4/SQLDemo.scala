package cn.itcast.spark.day4

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author y15079
  * @create 2018-03-11 14:23
  * @desc
  **/
object SQLDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDemo").setMaster("local[2]")
    //val conf = new SparkConf().setAppName("SQLDemo") 要打包则不需要后面的setMaster("local[2]")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name", "bigdata")

    //hdfs://node-1.itcast.cn:9000/person.txt
    val personRdd = sc.textFile("D:\\IDEA\\HelloSpark\\src\\main\\files\\day4\\person.txt").map({line =>
      val fields = line.split(",")
      Person(fields(0).toLong, fields(1), fields(2).toInt)
    })

    //导入隐式转换，如果不导入无法将RDD转换成DataFrame
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDf = personRdd.toDF()

    //采用SQL编写风格
    personDf.registerTempTable("person")

    sqlContext.sql("select * from person where age >= 20 order by age desc limit 2").show()


  }
}

case class Person(id:Long, name:String, age:Int)
