package cn.itcast

/**
  * @author y15079
  * @create 2018-03-07 12:56
  * @desc
  **/
object TestHash {
  def main(args: Array[String]): Unit = {
    val key = "net.itcast.cn"
    val mod = 3
    val rawMod = key.hashCode % mod
    val partNum = rawMod + (if (rawMod < 0) mod else 0)
    println(partNum)
  }
}
