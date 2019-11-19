package com.cycloneboy.scala.spark.rdd

import org.apache.spark.Partitioner

/**
 *
 * Create by  sl on 2019-11-19 16:13
 *
 * 实现自定义的分区器
 */
class CustomerPartitioner(numParts: Int) extends Partitioner {

  override def numPartitions: Int = numParts

  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    val ckey: String = key.toString
    ckey.substring(ckey.length - 1).toInt % numParts
  }
}
