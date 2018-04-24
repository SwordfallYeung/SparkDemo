package cn.itcast.spark.day5

import org.apache.log4j.{Level, Logger}

/**
  * @author y15079
  * @create 2018-03-12 0:10
  * @desc
  *      参考https://blog.csdn.net/a123demi/article/details/72821488 有三种日志控制写法
  **/
object LoggerLevels {
  def setStreamingLogLevels(): Unit ={
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized){
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}

//spark 1.6 版本的日志级别控制写法
/*
import org.apache.spark.Logging

object LoggerLevels extends Logging{
  def setStreamingLogLevels(): Unit ={
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized){
      logInfo("Setting log level to [WARN] for streaming example." + " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}*/
