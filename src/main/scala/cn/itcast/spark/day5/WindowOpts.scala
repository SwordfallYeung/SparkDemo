package cn.itcast.spark.day5

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
  * @author y15079
  * @create 2018-03-13 10:55
  * @desc
  **/
object WindowOpts {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("WindowOpts").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Milliseconds(5000))
    val lines = ssc.socketTextStream("192.168.187.201", 9999)
    val pairs = lines.flatMap(_.split(" ")).map((_, 1))
    val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int, b:Int) => (a + b), Seconds(15), Seconds(10)) //第一个参数是聚合，第二个参数是窗口长度，第三个参数是窗口滑动间隔
    //Map((hello, 5),(jerry, 2), (kitty, 3))
    windowedWordCounts.print()
    /*
    val a = windowedWordCounts.map(_._2).reduce(_+_)
    a.foreachRDD(rdd => {
      println(rdd.take(0))
    })
    a.print()
    windowedWordCounts.map(t => (t._1, t._2.toDouble / a.toD))
    */
    ssc.start()
    ssc.awaitTermination()
  }
}
