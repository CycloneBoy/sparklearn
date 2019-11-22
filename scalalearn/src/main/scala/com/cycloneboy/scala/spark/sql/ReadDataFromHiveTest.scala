package com.cycloneboy.scala.spark.sql

import org.apache.spark.sql.SparkSession

/**
 *
 * Create by  sl on 2019-11-20 12:39
 *
 * spark sql 操作hive
 */
object ReadDataFromHiveTest {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ReadDataFromHiveTest")
      //      .config("spark.sql.warehouse.dir", "hdfs://localhost/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 简单sql
    println("--------------------简单sql-----------------------")
    spark.sql("show databases").show
    spark.sql("show tables").show
    spark.sql("use default").show()
    spark.sql("select * from student").show()

    spark.sql("create table if not exists student3\nas select id, name from student")


    spark.stop()

  }
}
