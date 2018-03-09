package cn.itcast.spark.day3

import java.net.URL

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author y15079
  * @create 2018-03-07 11:28
  * @desc
  **/
object UrlCountPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //rdd1将数据切分，元组中放的是（url, 1）
    val rdd1 = sc.textFile("D:\\IDEA\\HelloSpark\\src\\main\\files\\usercount\\itcast.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })
    val rdd2 = rdd1.reduceByKey(_+_)

    //没有缓存
    /*val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, (url, t._2))
    })*/

    //缓存
    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, (url, t._2))
    }).cache() //cache会将数据缓存到内存当中，cache是一个transformation ,延迟执行 ，缓存提高性能

    val ints = rdd3.map(_._1).distinct().collect() //distinct去重

    val hostPartitioner = new HostPartitioner(ints)

    val rdd4 = rdd3.partitionBy(hostPartitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })

    val rdd5 = rdd3.partitionBy(new HashPartitioner(ints.length))
    rdd4.saveAsTextFile("d://out")
    //println(rdd4.collect().toBuffer)
    sc.stop()
  }
}

/**
  * 决定了数据到哪个分区里面
  * @param ins
  */
class HostPartitioner(ins: Array[String]) extends Partitioner{

  val parMap = new mutable.HashMap[String, Int]()
  var count = 0
  for (i <- ins){
    parMap += (i -> count)
    count += 1
  }

  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString, 0)
  }
}