package cn.itcast.spark.day2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author y15079
  * @create 2018-03-06 20:44
  * @desc
  **/
object ForeachDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachDemo").setMaster("local") //本地跑
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 3);
    rdd1.foreachPartition(it => {

    })
    sc.stop()
  }
}
