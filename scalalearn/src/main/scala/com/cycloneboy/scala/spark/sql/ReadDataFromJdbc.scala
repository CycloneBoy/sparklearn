package com.cycloneboy.scala.spark.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 *
 * Create by  sl on 2019-11-20 12:20
 *
 * Spark SQL可以通过JDBC从关系型数据库中读取数据的方式创建DataFrame，
 * 通过对DataFrame一系列的计算后，还可以将数据再写回关系型数据库中。
 */
object ReadDataFromJdbc {

  //3.定义连接mysql的参数
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/mysqlsource"
  val userName = "root"
  val passWd = "123456"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("ReadDataFromJdbc").getOrCreate()

    //从Mysql数据库加载数据方式一
    println("----------------从Mysql数据库加载数据方式一---------------------")
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mysqlsource")
      .option("dbtable", "student")
      .option("user", "root")
      .option("password", "123456")
      .load().cache()

    jdbcDF.printSchema()
    jdbcDF.show()


    // 从Mysql数据库加载数据方式二
    println("----------------从Mysql数据库加载数据方式二---------------------")
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:mysql://localhost:3306/mysqlsource", "student", connectionProperties).cache()

    jdbcDF2.printSchema()
    jdbcDF2.show()


    // 将数据写入Mysql方式一
    println("----------------将数据写入Mysql方式一---------------------")
    val filterDf1 = jdbcDF.filter("id > 5")
    filterDf1.show()

    filterDf1.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mysqlsource")
      .option("dbtable", "student1")
      .option("user", "root")
      .option("password", "123456")
      .save()

    // 将数据写入Mysql方式二
    println("----------------将数据写入Mysql方式二---------------------")
    jdbcDF2.write
      .jdbc("jdbc:mysql://localhost:3306/mysqlsource", "student2", connectionProperties)
  }
}
