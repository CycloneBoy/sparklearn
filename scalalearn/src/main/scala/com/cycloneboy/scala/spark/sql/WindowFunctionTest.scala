package com.cycloneboy.scala.spark.sql

import org.apache.spark.sql.SparkSession

/**
 *
 * Create by  sl on 2019-11-25 11:31
 *
 * 窗口函数
 */
case class Score(name: String, clasz: Int, score: Int)

object WindowFunctionTest {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("WindowFunctionTest")
      //      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("error")

    val scoreArray = Array(Score("a1", 1, 80),
      Score("a2", 1, 78),
      Score("a3", 1, 95),
      Score("a4", 2, 74),
      Score("a5", 2, 92),
      Score("a6", 3, 99),
      Score("a7", 3, 99),
      Score("a8", 3, 45),
      Score("a9", 3, 55),
      Score("a10", 3, 78))

    import sparkSession.implicits._
    val scoreDF = sparkSession.sparkContext.makeRDD(scoreArray).toDF("name", "clasz", "score").cache()
    scoreDF.createOrReplaceTempView("studentscore")
    scoreDF.show()

    println("'----------------标准允许将所有聚合函数用做聚合开窗函数--------------------")
    sparkSession.sql("select name,clasz,score ,count(name) over() name_count  from studentscore").show()

    println("'----------------用于显示按照班级分组后每组的人数--------------------")
    sparkSession.sql("select name,clasz,score ,count(name) over(partition by clasz) name_count  from studentscore").show()


    println("----------------------排序开窗函数----------------------")

    println("--------------------------于开窗函数分别为：ROW_NUMBER（行号）-------------------------------")
    sparkSession.sql("select name, clasz, score, row_number() over(order by score) rank from studentscore").show()

    println("--------------------------于开窗函数分别为：RANK（排名）-------------------------------")
    sparkSession.sql("select name, clasz, score, rank() over(order by score) rank from studentscore").show()

    println("--------------------------于开窗函数分别为：DENSE_RANK（密集排名）-------------------------------")
    sparkSession.sql("select name, clasz, score, dense_rank() over(order by score) rank from studentscore").show()

    println("--------------------------于开窗函数分别为： NTILE（分组排名）-------------------------------")
    sparkSession.sql("select name, clasz, score, ntile(6) over(order by score) rank from studentscore").show()

    println("----------------------排序函数和聚合开窗函数类似，也支持在 OVER 子句中使用 PARTITION BY 语句----------------------")

    println("------------partition by clasz order by score--------")
    sparkSession.sql("select name, clasz, score, row_number() over(partition by clasz order by score) rank from studentscore").show()
    sparkSession.sql("select name, clasz, score, rank() over(partition by clasz order by score) rank from studentscore").show()
    sparkSession.sql("select name, clasz, score, dense_rank() over(partition by clasz order by score) rank from studentscore").show()
    sparkSession.sql("select name, clasz, score, ntile(6) over(partition by clasz order by score) rank from studentscore").show()

  }
}
