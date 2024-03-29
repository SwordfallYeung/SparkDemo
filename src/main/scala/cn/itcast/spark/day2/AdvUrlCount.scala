package cn.itcast.spark.day2

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author y15079
  * @create 2018-03-07 10:37
  * @desc
  **/
object AdvUrlCount {
  def main(args: Array[String]): Unit = {

    //从数据库中加载规则
    val arr = Array("java.itcast.cn", "php.itcast.cn", "net.itcast.cn")

    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //rdd1将数据切分，元组中放的是（url, 1）
    val rdd1 = sc.textFile("D:\\IDEA\\HelloSpark\\src\\main\\files\\usercount\\itcast.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })
    val rdd2 = rdd1.reduceByKey(_+_)

    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, url, t._2)
    })

    //println(rdd3.collect().toBuffer)
    /*val rddJava = rdd3.filter(_._1 == "java.itcast.cn")
    val sortdJava = rddJava.sortBy(_._3, false).take(3)
    println(sortdJava.toBuffer)*/

    for (ins <- arr){
      val rdd = rdd3.filter(_._1 == ins)
      val sortd = rdd.sortBy(_._3, false).take(3)
      //通过JDBC向数据库中存储数据
      //id，学院，URL，次数，访问日期
    }
    sc.stop()
  }
}
