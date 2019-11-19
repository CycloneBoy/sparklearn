package com.cycloneboy.scala.spark.sql

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Create by  sl on 2019-11-19 21:25
 *
 * RDD转换为DateFrame 三种方式
 */
case class Student(id: Long, name: String)

object SparkSqlMain {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("AdRddPractice") //设置本程序名称

    //Spark程序的编写都是从SparkContext开始的
    val sc = new SparkContext(conf)

    val ss = SparkSession.builder
      .master("local[*]")
      .appName("SparkSqlMain")
      .getOrCreate()

    val data = sc.textFile("hdfs://localhost:9000/student/student.txt").cache()

    data.take(5) foreach println
    println()

    // 通过手动确定转换
    println("---------通过手动确定转换----------")
    import ss.implicits._
    val data1 = data.map { line =>
      val p = line.split("\t")
      (p(0).toInt, p(1))
    }.toDF("id", "name")

    data1.show(5)
    println()

    // 通过反射确定（需要用到样例类）
    println("---------通过反射确定（需要用到样例类）----------")
    val data2 = data.map { line =>
      val p = line.split("\t")
      Student(p(0).toInt, p(1))
    }.toDF

    data2.show()
    println()

    // 通过编程的方式
    println("---------通过编程的方式----------")
    val studentType: StructType = StructType(StructField("id", LongType) :: StructField("name", StringType) :: Nil)

    val data3 = data.map { x =>
      val p = x.split("\t")
      Row(p(0).toLong, p(1))
    }

    val dataFrame = ss.createDataFrame(data3, studentType)
    dataFrame.show()

    // 将DataFrame转换为RDD
    println("---------将DataFrame转换为RDD----------")
    val data4 = dataFrame.rdd

    data4.collect foreach println

    // 将RDD转换为DataSet
    println("---------将RDD转换为DataSet----------")
    val data5 = data.map { line =>
      val p = line.split("\t")
      Student(p(0).toInt, p(1))
    }.toDS()

    data5.show()
    println()

    //  IDEA创建SparkSQL程序
    println("--------- IDEA创建SparkSQL程序----------")

    data5.filter($"id" > 1010).show()

    data5.createOrReplaceTempView("students")

    ss.sql("SELECT * FROM students where id > 1013").show()


    sc.stop()

  }

}
