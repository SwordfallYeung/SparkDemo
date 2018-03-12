package cn.itcast.spark.day5

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author y15079
  * @create 2018-03-12 18:34
  * @desc
  **/
object FlumePushWordCount {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("FlumeWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //推送方式：flume向spark发送数据 192.168.187.200为spark程序所在的机器ip
    val flumeStream = FlumeUtils.createStream(ssc, "192.168.187.200", 8888)
    //flume中的数据通过event.getBody()才能拿到真正的内容
    val words = flumeStream.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_,1))
    val results = words.reduceByKey(_+_)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
