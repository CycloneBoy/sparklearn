package com.cycloneboy.scala.spark.rdd

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * create by CycloneBoy on 2019-01-20 10:24
  */
object PersistRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("PersistRdd")
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List(1, 2, 3, 4))
    // 获取分区数量
    println(input.partitions.size)
    val result = input.map(x => x * x)
    // 缓存
    result.persist(StorageLevel.DISK_ONLY)
    println(result.count())
    println(result.collect().mkString(","))


  }
}
