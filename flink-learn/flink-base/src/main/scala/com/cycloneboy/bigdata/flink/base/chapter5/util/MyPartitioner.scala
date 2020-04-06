package com.cycloneboy.bigdata.flink.base.chapter5.util

import org.apache.flink.api.common.functions.Partitioner

import scala.util.Random

/**
 *
 * Create by  sl on 2020-04-06 11:05
 */
object MyPartitioner extends Partitioner[Int] {

  val random: Random.type = scala.util.Random

  override def partition(key: Int, numPartitions: Int): Int = {

    if (key < 0) 0 else random.nextInt(numPartitions)
  }

}
