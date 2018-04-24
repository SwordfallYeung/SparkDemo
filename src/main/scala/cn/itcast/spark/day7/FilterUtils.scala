package cn.itcast.spark.day7

import org.apache.commons.lang3.time.FastDateFormat

/**
  * @author y15079
  * @create 2018-04-20 21:16
  * @desc
  **/
object FilterUtils {

  //线程安全
  val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")

  def filterByTime(fields: Array[String], startTime: Long, endTime: Long):Boolean = {
    val time = fields(1)
    val logTime = dateFormat.parse(time).getTime
    logTime >= startTime && logTime < endTime
  }

  def filterByType(fields: Array[String], eventType:String):Boolean = {
    val _type = fields(0)
    eventType == _type
  }

  def filterByTypes(fields: Array[String], eventTypes: String*): Boolean = {
    val _type = fields(0)
    for (et <- eventTypes){
      if (_type == et)
        return true
    }
    false
  }

  def filterByTypeAndTime(fields: Array[String], eventType: String, beginTime:Long, endTime: Long): Boolean = {
    val _type = fields(0)
    val _time = fields(1)
    val logTime = dateFormat.parse(_time).getTime
    eventType == _type && logTime >= beginTime && logTime < endTime
  }

}
