package com.cycloneboy.scala.spark.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/**
 *
 * Create by  sl on 2019-11-20 11:56
 */
// 定义自定义聚合函数
object MyEmployeeAverage extends Aggregator[Employee, Average, Double] {

  /**
   * 定义一个数据结构，保存工资总数和工资总个数，初始都为0
   *
   * @return
   */
  override def zero: Average = Average(0L, 0L)

  /**
   * 聚合不同execute的结果
   *
   * @param b
   * @param a
   * @return
   */
  override def reduce(b: Average, a: Employee): Average = {
    b.sum += a.salary
    b.count += 1
    b
  }

  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  /**
   * 计算输出
   *
   * @param reduction
   * @return
   */
  override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  /**
   * 设定之间值类型的编码器，要转换成case类
   * Encoders.product是进行scala元组和case类转换的编码器
   *
   * @return
   */
  override def bufferEncoder: Encoder[Average] = Encoders.product

  /**
   * 设定最终输出值的编码器
   *
   * @return
   */
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}