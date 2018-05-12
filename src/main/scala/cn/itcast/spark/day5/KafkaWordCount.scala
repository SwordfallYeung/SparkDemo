package cn.itcast.spark.day5

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author y15079
  * @create 2018-03-13 9:27
  * @desc
  **/
object KafkaWordCount {

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it => Some(it._2.sum + it._3.getOrElse(0)).map(x => (it._1, x)))
    iter.flatMap{case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x,i))}
  }

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    //node-1.itcast.cn:2181,node-2.itcast.cn:2181,node-3.itcast.cn:2181 g1 test 2
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // ssc.checkpoint封装了sc.setCheckpointDir("d://ck")在里面
    ssc.checkpoint("d://ck1")   //单机运行可以，在集群运行时目录要改为hdfs目录
    //"alog-2016-04-16, alog-2016-04-17, alog-2016-04-18
    //Array((alog-2016-04-16, 2), (alog-2016-04-17, 2), (alog-2016-04-18, 2))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    data.print()
    val words = data.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    //wordCounts.print()
    wordCounts.mapPartitions(it => { //一般写入到数据库或redis时才需要mapPartitions
      //Jedis连接，
      it
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

