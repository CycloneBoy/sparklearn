package com.cycloneboy.scala.spark.rdd

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
 *
 * Create by  sl on 2019-11-25 12:10
 *
 * 用户自定义聚合函数
 */
class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
  /**
   * 聚合函数输入参数的数据类型
   */
  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType) ::
    Nil)

  /**
   * 聚合缓冲区中值的类型
   * 中间进行聚合时所处理的数据类型
   */
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo",
    StringType) :: Nil)

  /**
   * 函数返回值的数据类型
   */
  override def dataType: DataType = StringType

  /**
   * 一致性检验，如果为 true，那么输入不变的情况下计算的结果也是不变的
   */
  override def deterministic: Boolean = true

  /**
   * 设置聚合中间 buffer 的初始值
   * 需要保证这个语义：两个初始 buffer 调用下面实现的 merge 方法后也应该为初始 buffer 即如果你初始值是
   * 1，然后你 merge 是执行一个相加的动作，两个初始 buffer 合并之后等于 2，不会等于初始 buffer 了。这样的初始
   * 值就是有问题的，所以初始值也叫"zero value"
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  /**
   * 用输入数据 input 更新 buffer 值,类似于 combineByKey
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 缓冲中的已经拼接过的城市信息串
    var bufferCityInfo = buffer.getString(0)
    // 刚刚传递进来的某个城市信息
    val cityInfo = input.getString(0)
    // 在这里要实现去重的逻辑
    // 判断：之前没有拼接过某个城市信息，那么这里才可以接下去拼接新的城市信息

    if (!bufferCityInfo.contains(cityInfo)) {
      if ("".equals(bufferCityInfo))
        bufferCityInfo += cityInfo
      else {
        // 比如 1:北京
        // 1:北京,2:上海
        bufferCityInfo += "," + cityInfo
      }
      buffer.update(0, bufferCityInfo)
    }
  }

  /**
   * 合并两个 buffer,将 buffer2 合并到 buffer1.在合并两个分区聚合结果的时候会被用到,类似于
   * reduceByKey
   * 这里要注意该方法没有返回值，在实现的时候是把 buffer2 合并到 buffer1 中去，你需要实现这个合并细节
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0);
    val bufferCityInfo2 = buffer2.getString(0);
    for (cityInfo <- bufferCityInfo2.split(",")) {
      if (!bufferCityInfo1.contains(cityInfo)) {
        if ("".equals(bufferCityInfo1)) {
          bufferCityInfo1 += cityInfo;
        } else {
          bufferCityInfo1 += "," + cityInfo;
        }
      }
    }
    buffer1.update(0, bufferCityInfo1);
  }

  /**
   * 计算并返回最终的聚合结果
   */
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
