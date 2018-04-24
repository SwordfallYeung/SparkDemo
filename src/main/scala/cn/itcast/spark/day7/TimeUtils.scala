package cn.itcast.spark.day7

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * @author y15079
  * @create 2018-04-20 21:33
  * @desc
  **/
object TimeUtils {
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val calendar = Calendar.getInstance()

  def apply(time: String) = {
    calendar.setTime(simpleDateFormat.parse(time))
    calendar.getTimeInMillis
  }

  def getCertainDayTime(amount: Int):Long = {
    calendar.add(Calendar.DATE, amount)
    val time = calendar.getTimeInMillis
   /* calendar.add(Calendar.DATE, -amount)*/
    time
  }
}
