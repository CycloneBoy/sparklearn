package com.cycloneboy.scala.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Create by  sl on 2019-11-19 15:27
 *
 * 需求：统计出每一个省份广告被点击次数的TOP3
 *
 * 数据格式: 时间戳，省份，城市，用户，广告
 */
case class AdLog(ts: Long, province: Int, city: Int, user: Int, ad: Int)

object AdRddPractice {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("AdRddPractice") //设置本程序名称

    //Spark程序的编写都是从SparkContext开始的
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://localhost:9000/user/sl/spark/agent.log").filter(!_.contains("时间戳"))

    val data = lines.map { line =>
      val p = line.split(" ")
      AdLog(p(0).toLong, p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt)
    }.cache()

    data.take(5) foreach println
    println()

    //3.按照最小粒度聚合：((Province,AD),1)
    val provinceAdToOne = data.map(d => ((d.province, d.ad), 1))

    //4.计算每个省中每个广告被点击的总数：((Province,AD),sum)
    val provinceAdToSum = provinceAdToOne.reduceByKey(_ + _)

    //5.将省份作为key，广告加点击数为value：(Province,(AD,sum))
    val provinceToAdSum = provinceAdToSum.map(x => (x._1._1, (x._1._2, x._2)))

    //6.将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val provinceGroup = provinceToAdSum.groupByKey()

    //7.对同一个省份所有广告的集合进行排序并取前3条，排序规则为广告点击总数
    val provinceAdTop3 = provinceGroup.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }

    //8.将数据拉取到Driver端并打印
    provinceAdTop3.collect().foreach(println)

  }
}
