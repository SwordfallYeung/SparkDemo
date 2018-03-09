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

    //textFile会产生两个RDD 1.HadoopRDD -> MapPartitionRDD
    sc.textFile(args(0))
      //产生一个RDD：MapPartitionRDD
      .flatMap(_.split(" "))
      //产生一个RDD：MapPartitionRDD
      .map((_,1))
      //产生一个RDD：ShuffleRDD 下游从上游拉取数据
      .reduceByKey(_+_).sortBy(_._2,false)
      //产生一个RDD：ShuffleRDD
      .saveAsTextFile(args(1))
    sc.stop()
  }
}
