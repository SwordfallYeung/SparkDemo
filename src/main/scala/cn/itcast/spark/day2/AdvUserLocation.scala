package cn.itcast.spark.day2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author y15079
  * @create 2018-03-07 1:20
  * @desc
  **/
object AdvUserLocation {
  def main(args: Array[String]): Unit = {
    //配置环境变量后需要重启电脑才能生效，可以直接在程序配置环境变量 ，参考https://www.cnblogs.com/hyl8218/p/5492450.html
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.1")
    val conf = new SparkConf().setAppName("ForeachDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //sc.textFile("c://bs_log").map(_.split(",")).map(x => (x(0), x(1), x(2), x(3))) //需要在window下配置hadoop本地库，否则会报错
    val rdd0 = sc.textFile("D:\\IDEA\\HelloSpark\\src\\main\\files\\userlocal").map( line => {
      val fields = line.split(",")
      val eventType = fields(3)
      val time = fields(1)
      val timeLong = if (eventType == "1") -time.toLong else time.toLong
      ((fields(0), fields(2)), timeLong)
    })
    val rdd1 = rdd0.reduceByKey(_+_).map(t => {
      val mobile = t._1._1
      val lac = t._1._2
      val time = t._2
      (lac, (mobile, time))
    })
    val rdd2 = sc.textFile("D:\\IDEA\\HelloSpark\\src\\main\\files\\lac_info.txt").map(line => {
      val f = line.split(",")
      //(基站)， (经度，纬度)
      (f(0), (f(1), f(2)))
    })
    val rdd3 = rdd1.join(rdd2).map(t => {
      val lac = t._1
      val mobile = t._2._1._1
      val time = t._2._1._2
      val x = t._2._2._1
      val y = t._2._2._2
      (mobile, lac, time, x, y)
    })
    val rdd4 = rdd3.groupBy(_._1)
    val rdd5 = rdd4.mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(2)
    })
    println(rdd5.collect().toBuffer)
    rdd5.saveAsTextFile("d:\\out")
   sc.stop()
  }
}
