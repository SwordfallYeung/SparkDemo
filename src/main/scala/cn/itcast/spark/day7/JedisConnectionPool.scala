package cn.itcast.spark.day7

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @author y15079
  * @create 2018-04-21 9:20
  * @desc 单例 redis连接池
  **/
object JedisConnectionPool extends Serializable {

  val config = new JedisPoolConfig()
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查
  config.setTestOnBorrow(false)
  val pool = new JedisPool(config, "192.168.187.201", 6379)

  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn = JedisConnectionPool.getConnection()
    //获取所有的keys
    val keys = conn.keys("*")
    println(keys)

    println(conn.get("无法无天_1524306530221"))
    /*val r = conn.set("hello", "world")
    println(r)
    println(conn.ping())*/
  }
}
