package com.cycloneboy.scala.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Create by  sl on 2019-11-19 20:52
 *
 * 自定义累加器
 * 实现自定义类型累加器需要继承AccumulatorV2并至少覆写下例中出现的方法，<br/>
 * 下面这个累加器可以用于在程序运行过程中收集一些文本类信息，最终以Set[String]的形式返回
 */
object LogAccumulatorTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("LogAccumulatorTest")

    val sc = new SparkContext(conf)

    val accum = new LogAccumulator
    sc.register(accum, "logAccum")

    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if (!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum: " + sum)
    println(accum.value.toString)
    println()
    sc.stop()
  }
}
