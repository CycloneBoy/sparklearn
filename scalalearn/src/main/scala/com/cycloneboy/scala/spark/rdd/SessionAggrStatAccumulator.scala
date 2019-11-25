package com.cycloneboy.scala.spark.rdd

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 *
 * Create by  sl on 2019-11-25 12:08
 *
 * 自定义AccumulatorV2
 *
 */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String,
  Int]] {
  // 保存所有聚合数据
  private val aggrStatMap = mutable.HashMap[String, Int]()
  // 判断是否为初始值

  override def isZero: Boolean = {
    aggrStatMap.isEmpty
  }

  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionAggrStatAccumulator
    aggrStatMap.synchronized {
      newAcc.aggrStatMap ++= this.aggrStatMap
    }
    newAcc
  }

  // 重置累加器中的值
  override def reset(): Unit = {
    aggrStatMap.clear()
  }

  // 向累加器中添加另一个值
  override def add(v: String): Unit = {
    if (!aggrStatMap.contains(v))
      aggrStatMap += (v -> 0)
    aggrStatMap.update(v, aggrStatMap(v) + 1)
  }

  // 各个 task 的累加器进行合并的方法
  // 合并另一个类型相同的累加器
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc: SessionAggrStatAccumulator => {
        (this.aggrStatMap /: acc.value) { case (map, (k, v)) => map += (k -> (v + map.getOrElse(k,
          0)))
        }
      }
    }
  }

  // 获取累加器中的值
  // AccumulatorV2 对外访问的数据结果
  override def value: mutable.HashMap[String, Int] = {
    this.aggrStatMap
  }
}
