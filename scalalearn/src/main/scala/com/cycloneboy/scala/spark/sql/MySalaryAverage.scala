package com.cycloneboy.scala.spark.sql

import org.apache.spark.sql.SparkSession

/**
 *
 * Create by  sl on 2019-11-20 11:37
 *
 * 展示一个求平均工资的自定义聚合函数
 */
object MySalaryAverage {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .master("local[*]")
      .appName("MySalaryAverage")
      .getOrCreate()


    // 注册函数
    ss.udf.register("myAverage", MyAverage)

    val df = ss.read.json("hdfs://localhost:9000/user/sl/spark/salary.json")
    df.createOrReplaceTempView("employees")
    df.show()

    val result = ss.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
  }
}
