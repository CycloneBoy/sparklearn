package com.cycloneboy.scala.spark.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Create by  sl on 2020-01-22 17:03
 */
object WordCount1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount1")

    val sc = new SparkContext(conf)

    val data: RDD[String] = sc.textFile("hdfs://localhost:9000/user/sl/README.md")

  }
}
