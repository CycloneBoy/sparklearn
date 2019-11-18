package com.cycloneboy.scala.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Create by  sl on 2019-11-18 12:06
 */
object TransmitTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TransmitTest").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.parallelize(Array("hadoop", "spark", "hive", "sl"))

    val search = new Search("hive")

    val match1 = search.getMatch2(rdd)
    match1.collect().foreach(println)
  }
}
