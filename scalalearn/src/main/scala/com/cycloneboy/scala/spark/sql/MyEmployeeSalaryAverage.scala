package com.cycloneboy.scala.spark.sql

import org.apache.spark.sql.SparkSession

/**
 *
 * Create by  sl on 2019-11-20 11:37
 *
 * 展示一个求平均工资的自定义聚合函数
 * 强类型用户自定义聚合函数：
 * 通过继承Aggregator来实现强类型自定义聚合函数，同样是求平均工资
 */

// 既然是强类型，可能有case类
case class Employee(name: String, salary: Long)

case class Average(var sum: Long, var count: Long)


object MyEmployeeSalaryAverage {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .master("local[*]")
      .appName("MyEmployeeSalaryAverage")
      .getOrCreate()


    // 注册函数
    import ss.implicits._
    val ds = ss.read.json("hdfs://localhost:9000/user/sl/spark/salary.json").as[Employee]
    ds.show()

    val averageSalary = MyEmployeeAverage.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()
  }
}
