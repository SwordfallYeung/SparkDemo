package cn.itcast

/**
  * @author y15079
  * @create 2018-03-08 11:09
  * @desc
  **/
object Demo {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,2,3,5,7,9,11,13,15)
    for (i <- arr){
      for (x <- arr){
        for (y <- arr){
          val count = i + x + y
          if (count == 30){
            println(s"$i, $x, $y 这三个数的和为30")
          }
        }
      }
    }
  }
}
