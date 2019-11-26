package com.cycloneboy.bigdata.businessanalysis.analyse.session

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 *
 * Create by  sl on 2019-11-26 14:03
 *
 * 自定义累加器
 */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  private val aggrStatMap = mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    aggrStatMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionAggrStatAccumulator
    aggrStatMap.synchronized {
      newAcc.aggrStatMap ++= this.aggrStatMap
    }
    newAcc
  }

  override def reset(): Unit = {
    aggrStatMap.clear()
  }

  override def add(v: String): Unit = {
    if (!aggrStatMap.contains(v)) {
      aggrStatMap += (v -> 0)
    }
    aggrStatMap.update(v, aggrStatMap(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc: SessionAggrStatAccumulator => {
        (this.aggrStatMap /: acc.value) { case (map, (k, v)) => map += (k -> (v + map.getOrElse(k, 0))) }
        //        this.aggrStatMap.foldLeft(acc.value) {
        //          case (map, (k, v)) => map += (k -> (v + map.getOrElse(k, 0)))
        //        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.aggrStatMap
  }
}
