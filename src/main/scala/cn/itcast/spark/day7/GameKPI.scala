package cn.itcast.spark.day7

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author y15079
  * @create 2018-04-20 21:32
  * @desc
  **/
object GameKPI {

  def main(args: Array[String]): Unit = {

    val queryTime = "2016-02-02 00:00:00"
    val beginTime = TimeUtils(queryTime)
    val endTime = TimeUtils.getCertainDayTime(-1)

    val conf = new SparkConf().setAppName("GameKPI").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //切分之后的数据
    val splitedLogs = sc.textFile(args(0)).map(_.split("\\|"))
    //过滤后并缓冲
    val filteredLogs = splitedLogs.filter(fields => FilterUtils.filterByTime(fields, beginTime, endTime)).cache()

    //日新增用户数，Daily New Users 缩写DNU
    val dnu = filteredLogs.filter(fields => FilterUtils.filterByType(fields, EventType.REGISTER)).count()

    /*println(dnu)*/

    //日活跃用户数 DAU (Daily Active Users)
    val dau = filteredLogs.filter(fields => FilterUtils.filterByTypes(fields, EventType.REGISTER, EventType.LOGIN))
      .map(_(3))
      .distinct()
      .count()

    //  留存率：某段时间的新增用户数记为A，经过一段时间后，仍然使用的用户占新增用户A的比例即为留存率
    //  次日留存率（Day 1 Retention Ratio） Retention [rɪ'tenʃ(ə)n] Ratio ['reɪʃɪəʊ]
    //  日新增用户在+1日登陆的用户占新增用户的比例
    val t1 = TimeUtils.getCertainDayTime(-1)
    val lastDayRegUser = splitedLogs.filter(fields => FilterUtils.filterByTypeAndTime(fields, EventType.REGISTER, t1, beginTime))
          .map(x => (x(3), 1))
    val todayLoginUser = filteredLogs.filter(fields => FilterUtils.filterByType(fields, EventType.LOGIN))
          .map(x => (x(3), 1))
          .distinct()

    val dlr: Double = lastDayRegUser.join(todayLoginUser).count()
    println(dlr)
    val dlrr = dlr /lastDayRegUser.count()
    println(dlrr)

    sc.stop()


  }

}
