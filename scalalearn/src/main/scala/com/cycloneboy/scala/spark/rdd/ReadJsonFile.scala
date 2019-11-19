package com.cycloneboy.scala.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
 *
 * Create by  sl on 2019-11-19 16:22
 */
object ReadJsonFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AdRddPractice") //设置本程序名称

    //Spark程序的编写都是从SparkContext开始的
    val sc = new SparkContext(conf)

    val jsons = sc.textFile("hdfs://localhost:9000/user/sl/spark/user.json")

    val result = jsons.map(JSON.parseFull)

    result.collect foreach { x =>
      println(x.mkString(", "))
    }

  }
}
