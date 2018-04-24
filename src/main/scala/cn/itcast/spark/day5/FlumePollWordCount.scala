/*
package cn.itcast.spark.day5

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author y15079
  * @create 2018-03-12 22:31
  * @desc
  **/
object FlumePollWordCount {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("FlumePollWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //从flume中拉取数据(flume的地址)
    val address = Seq(new InetSocketAddress("192.168.187.201", 8888))
    val flumeStream = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK)
    val words = flumeStream.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_,1))
    val results = words.reduceByKey(_+_)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
*/
