package cn.itcast.spark.day3

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author y15079
  * @create 2018-03-08 23:52
  * @desc
  **/
object JdbcRDDDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "admin")
    }
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM ta where id >=? AND id <= ?",
      1, 4, 2,
      rs => {
        val id = rs.getInt(1)
        val code = rs.getString(2)
        (id, code)
      }
    )

    val jrdd = jdbcRDD.collect()
    println(jdbcRDD.collect().toBuffer)
    sc.stop()
  }
}
