/*
package cn.itcast.spark.day7

import java.util.Date

import cn.itcast.spark.day5.LoggerLevels
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author y15079
  * @create 2018-04-21 9:36
  * @desc 检测游戏外挂
  **/
object ScanPlugins {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = Array("hadoop1:2181,hadoop2:2181,hadoop3:2181", "g0", "gamelog", "1")
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val conf = new SparkConf().setAppName("ScanPlugins").setMaster("local[4]")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Milliseconds(5000))
    //receiver连接，使用checkpoint
    sc.setCheckpointDir("d://ck0")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //使用receiver方式从kafka获取数据，连接的是zookeeper
    //使用Direct直连方式，连接的是kafka broker
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )
    val dstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    val lines = dstream.map(_._2)

    //检测从kafka是否能获取到数据
    /*lines.foreachRDD(lines => {
      println("lines count: "+lines.count())
      lines.foreach(line => {
        println("lines: "+line)
      })
    })*/

    val splitedLines = lines.map(_.split("\t"))
    val filteredLines = splitedLines.filter(f => {
      val et = f(3)
      val item = f(8)
      et == "10" && item == "龙纹剑"
    })

    /*filteredLines.foreachRDD(lines => {
      println("filteredLines 龙纹剑 count: "+lines.count())
      lines.foreach(line => {
        println(line(0)+" "+line(1)+" "+line(2)+" "+line(3)+" "+line(4)+" "+ line(5)+" "+line(6)+" "+line(7)+" "+line(8)+" "+line(9)+" "+line(10)+" "+line(11)+" "+line(12))
      })
    })*/

    //时间窗DStream  DStream[(String, Iterable[Long])] String为用户玩家 Iterable[Long]为使用的时间
    val groupedWindow: DStream[(String, Iterable[Long])] = filteredLines.map(f => (f(7), dateFormat.parse(f(12)).getTime)).groupByKeyAndWindow(Milliseconds(15000), Milliseconds(10000))
    //把这段时间间隔使用次数少的过滤掉
    val filtered: DStream[(String, Iterable[Long])] = groupedWindow.filter(_._2.size >= 3)

    /*filtered.foreachRDD(lines => {
      println("时间窗 filtered 龙纹剑 count: "+lines.count())
      lines.foreach(line => {
        println(line._1+" "+line._2)
      })
    })*/

    val itemAvgTime = filtered.mapValues(it => {
      val list = it.toList.sorted
      val size = list.size //使用的次数
      val first = list(0) //第一次使用的时间
    /*  println("first time:" +first + " "+ dateFormat.format(new Date(first)))*/
      val last = list(size - 1) //最后一次使用的时间
      /*println("last time:" +last + " "+ dateFormat.format(new Date(last)))*/
      val cha : Double = last - first
      cha / size //平均每次使用的时间长
    })

    /*itemAvgTime.foreachRDD(lines => {
      println("道具使用的平均时间 itemAvgTime 龙纹剑 count: "+lines.count())
      lines.foreach(line => {
        println(line._1+" "+line._2)
      })
    })*/
    //结果为 无法无天 13817163.93442623  即 平均每3个小时会使用一次道具

    //过滤时间较短的  平均每次使用的时间长越短，则使用外挂的机率越大  DStream[(String, Double)] String为用户玩家 Double为道具使用的平均时长
     val badUser: DStream[(String, Double)] = itemAvgTime.filter(_._2 < 20000000) //20000000 即5个小时

     badUser.foreachRDD(rdd => {
       rdd.foreachPartition(it => {
         val connection = JedisConnectionPool.getConnection()
         it.foreach(t => {
           val user = t._1
           val avgTime = t._2
           val currentTime = System.currentTimeMillis()
           connection.set(user + "_" + currentTime, (avgTime/1000).toString)
         })
         //回收Jedis资源
         connection.close()
       })
     })

    ssc.start()
    ssc.awaitTermination()
  }
}*/
