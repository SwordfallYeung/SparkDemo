package cn.itcast.spark.day3

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author y15079
  * @create 2018-03-07 13:54
  * @desc
  **/
object OrderContext{
  /*implicit object GirlOrdering extends Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if (x.faceValue > y.faceValue) 1
      else if (x.faceValue == y.faceValue){
        if (x.age > y.age) -1 else 1
      }else -1
    }
  }*/

  implicit val GirlOrdering = new Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if (x.faceValue > y.faceValue) 1
      else if (x.faceValue == y.faceValue){
        if (x.age > y.age) -1 else 1
      }else -1
    }
  }
}

//sort =>规则 先按faceValue，比较年龄
//name，faceValue，age
object CustomSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("yuihatano", 90, 28, 1),("angelababy", 90, 27 ,2),("JuJingYi", 95, 22, 3)))
    import OrderContext._
    val rdd2 = rdd1.sortBy(x => Girl(x._2, x._3), false)//false为降序，true为升序
    println(rdd2.collect().toBuffer)
    sc.stop()
  }
}

/**
  * 第一种方式
  * @param faceValue
  * @param age
  */
/*case class Girl(val faceValue:Int , val age:Int) extends Ordered[Girl] with Serializable{
  override def compare(that: Girl): Int = {
    if (this.faceValue == that.faceValue) {
      that.age - this.age
    }else {
      this.faceValue - that.faceValue
    }
  }
}*/
//第二种方式 通过隐式转换完成排序
case class Girl(val faceValue:Int , val age:Int) extends Serializable