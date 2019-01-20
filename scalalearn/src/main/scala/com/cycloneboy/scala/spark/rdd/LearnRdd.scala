package com.cycloneboy.scala.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * create by CycloneBoy on 2019-01-20 09:59
  */
object LearnRdd {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("LearnRdd")//设置本程序名称

    //Spark程序的编写都是从SparkContext开始的
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println(result.collect().mkString(","))

    val lines = sc.parallelize(List("hello world", "hi"))
    val words = lines.flatMap(line => line.split(" "))
    words.first() // 返回"hello


  }
}
