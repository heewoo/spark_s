package com.test.spark

import org.apache.spark.rdd.RDD

/**
  * Created by yellomobile on 2017. 1. 23..
  */
object piar_example {

  // keys
  def key_value(rdd: RDD[String]): Unit = {
    // RDD [String]
    //    rdd.keyBy(x=>x.contains("kr")).take(10).foreach(println)
    //    val key = rdd.map(x=>(x.split("<url><loc>")(0),x)).take(10)
    //    key.foreach(println)

    val urlKey = rdd.map(x => (x.replace("<url><loc>", ""))).take(10)
    val resultUrl = urlKey.map(x=>(x.replace("</loc></url>","")))

    resultUrl.foreach(println)
  }

  def rdd_map(rdd: RDD[String]): Unit = {
    val lineLength = rdd.map(s => s.length)
    val total = lineLength.reduce((a, b) => a + b)
    println(lineLength.count())
    println(total)
  }


}
