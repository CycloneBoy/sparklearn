package com.cycloneboy.scala.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Create by  sl on 2019-11-19 16:15
 *
 * 自定义分区
 * 使用自定义的 Partitioner 是很容易的:只要把它传给 partitionBy() 方法即可。<br/>
 * Spark 中有许多依赖于数据混洗的方法，比如 join() 和 groupByKey()，<br/>
 * 它们也可以接收一个可选的 Partitioner 对象来控制输出数据的分区方式。
 */
object PartitionTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("PartitionTest")

    val sc = new SparkContext(conf)

    val data = sc.parallelize(Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)))


    val par = data.partitionBy(new CustomerPartitioner(2))

    val res1 = par.mapPartitionsWithIndex((index, items) => items.map((index, _)))

    res1.collect foreach println
  }

}
