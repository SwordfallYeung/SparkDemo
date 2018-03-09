package cn.itcast.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author y15079
  * @create 2018-03-05 0:59
  * @desc
  **/
object WordCount {
  def main(args: Array[String]): Unit = {
    //非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WC")
    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))
    sc.stop()
  }
}
