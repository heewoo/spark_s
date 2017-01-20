package com.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext



object develop {

  val conf = new SparkConf().setMaster("local").setAppName("heewoo")
  conf.set("spark.driver.host","10.0.4.193") // host 지정 //

  val sc = new SparkContext(conf)

  val input = sc.textFile("/Users/yellomobile/Desktop/sitemap-1.xml")

  def textSave(): Unit ={
    val word = input.flatMap(line => line.split(" "))
    val count = word.map(word => (word,1)).reduceByKey{case (x,y) => x+y}
    count.saveAsTextFile("/Users/yellomobile/Desktop/sitemap-heewoo.xml")
  }

  def textCount(): Unit ={
    println(input.count())
  }


  def createRDD(): Unit ={
    val lines = sc.parallelize(List("heewoo","test","spark"))
  }

  def filter(): Unit ={
    val url = input.filter(x=>x.contains("feed"))
      println(url.first())
      println(url.count())
  }

  def take(): Unit ={
    input.take(5).foreach(println)
  }


  def flatMap(): Unit ={
    val lines = sc.parallelize(List("heewoo world" , "hello" , "world"))
    val word = lines.flatMap(line => line.split(" "))

    println(word.first())
  }

  def main(args: Array[String]): Unit = {
    input.collect().foreach(println)
  }



}
