package com.cycloneboy.scala.spark.rdd

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/**
 *
 * Create by  sl on 2019-11-25 12:11
 */
// 定义 case 类
case class Employee(name: String, salary: Long)

case class Average(var sum: Long, var count: Long)

object MyAverage extends Aggregator[Employee, Average, Double] {
  /**
   * 计算并返回最终的聚合结果
   */
  def zero: Average = Average(0L, 0L)

  /**
   * 根据传入的参数值更新 buffer 值
   */
  def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  /**
   * 合并两个 buffer 值,将 buffer2 的值合并到 buffer1
   */
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  /**
   * 计算输出
   */
  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  /**
   * 设定中间值类型的编码器，要转换成 case 类
   * Encoders.product 是进行 scala 元组和 case 类转换的编码器
   */
  def bufferEncoder: Encoder[Average] = Encoders.product

  /**
   * 设定最终输出值的编码器
   */
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

