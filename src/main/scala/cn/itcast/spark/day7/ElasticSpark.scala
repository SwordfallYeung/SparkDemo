package cn.itcast.spark.day7

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * @author y15079
  * @create 2018-04-24 1:44
  * @desc spark rdd整合elasticSearch
  **/
object ElasticSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ElasticSpark").setMaster("local")
    conf.set("es.nodes","hadoop1,hadoop2,hadoop3")
    conf.set("es.port","9200")
    conf.set("es.index.auto.create","true")
    val sc = new SparkContext(conf)
    val start = 1463998397
    val end = 1463998399
    val tp = "1"
    val query: String =s"""{
         "query": {
                 "bool": {
                      "must": [
                          {"term": {"access.type": $tp}},
                          {
                             "range": {
                                 "access.time":{
                                    "gte": $start,
                                    "lte": $end
                                 }
                             }
                          }
                      ]
                  }
          }
       }"""

    //等同于以下的语句
    /*val query: String =s"""{
          "query": {
               "bool": {
                    "must": {
                        "term": {"access.type": "1"}
                    },
                    "filter":{
                          "range": {
                              "access.time": {
                                  "gte": "1463998397",
                                  "lte": "1463998399"
                              }
                          }
                    }
               }
          }
       }"""*/
    val rdd1 = sc.esRDD("accesslog",query);

    println(rdd1.collect().toBuffer)

    var rdd1Array = rdd1.collect()
    println(rdd1Array.size)

    for (a <- rdd1Array){
      println(a._1)
      for (m <- a._2){
        println(m._1 + " "+ m._2)
      }
    }
  }
}
