package com.cycloneboy.scala.spark.tempdata

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Create by  sl on 2019-11-19 10:11
 * 说明: 数据来源: https://download.bls.gov/pub/time.series/la/
 * 学习资料来源: https://www.youtube.com/watch?v=jzrT4NdPbH0&list=PLLMXbkbDbVt-f6qwCZqfq7e_6eT8aFxzT&index=25
 */

case class Area(code: String, text: String)

case class Series(id: String, area: String, measure: String, title: String)

case class LAData(id: String, year: Int, period: Int, value: Double)

object RDDUnemployment {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Unemployment data").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // 读取地区数据
    println("-------------读取地区数据--------------")
    val areas = sc.textFile("hdfs://localhost:9000/user/sl/spark/la.area").filter(!_.contains("area_type")).map { line =>
      val p = line.split("\t").map(_.trim)
      Area(p(1), p(2))
    }.cache()
    areas.take(5) foreach println

    // 读取雇佣关系数据
    println("-------------读取雇佣关系数据--------------")
    val series = sc.textFile("hdfs://localhost:9000/user/sl/spark/la.series").filter(!_.contains("area_code")).map { line =>
      val p = line.split("\t").map(_.trim)
      Series(p(0), p(2), p(3), p(6))
    }.cache()
    series.take(5) foreach println

    // 读取雇佣数据
    println("-------------读取雇佣数据--------------")
    val data = sc.textFile("hdfs://localhost:9000/user/sl/spark/la.data.30.Minnesota").filter(!_.contains("year")).map { line =>
      val p = line.split("\t").map(_.trim)
      LAData(p(0), p(1).toInt, p(2).drop(1).toInt, p(3).toDouble)
    }.cache()
    data.take(5) foreach println

    // 十年没有雇佣的员工
    println("-------------十年没有雇佣的员工--------------")
    val rates = data.filter(_.id.endsWith("03"))
    val decadeGroups = rates.map(d => (d.id, d.year / 10) -> d.value)
    val decadeAverages = decadeGroups.aggregateByKey(0.0 -> 0)({ case ((s, c), d) =>
      (s + d, c + 1)
    }, { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }).mapValues(t => t._1 / t._2).cache()

    decadeAverages.take(5) foreach println

    // 最大的十年
    println("-------------最大的十年--------------")
    val maxDecade = decadeAverages.map { case ((id, dec), av) => id -> (dec * 10, av) }.reduceByKey {
      case ((d1, a1), (d2, a2)) => if (a1 >= a2) (d1, a1) else (d2, a2)
    }
    maxDecade.take(1) foreach println

    val seriesPairs = series.map(s => s.id -> s.title)

    // 最大的十年 join series
    println("-------------最大的十年 join series--------------")
    val joinedMaxDecade = seriesPairs.join(maxDecade)
    joinedMaxDecade.take(10) foreach println

    // 最大的十年 join dataByArea
    println("-------------最大的十年 join dataByArea--------------")
    val dataByArea = joinedMaxDecade.mapValues { case (a, (b, c)) => (a, b, c) }.map {
      case (id, t) => id.drop(3).dropRight(2) -> t
    }
    dataByArea.take(5) foreach println

    // 最大的十年 join fullJoined
    println("------------- 最大的十年 join fullJoined--------------")
    val fullJoined = areas.map(a => a.code -> a.text).join(dataByArea)
    fullJoined.take(10) foreach println


    sc.stop()
  }
}
