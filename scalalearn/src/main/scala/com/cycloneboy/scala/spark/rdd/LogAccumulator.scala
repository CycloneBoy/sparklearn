package com.cycloneboy.scala.spark.rdd

import org.apache.spark.util.AccumulatorV2

/**
 *
 * Create by  sl on 2019-11-19 20:45
 */
class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]] {

  private var _logArray: java.util.Set[String] = new java.util.HashSet[String]()

  override def isZero: Boolean = _logArray.isEmpty

  override def copy(): AccumulatorV2[String, java.util.Set[String]] = {
    val newAcc = new LogAccumulator()
    _logArray.synchronized {
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }

  override def reset(): Unit = _logArray.clear()

  override def add(v: String): Unit = _logArray.add(v)

  override def merge(other: AccumulatorV2[String, java.util.Set[String]]): Unit = {
    other match {
      case o: LogAccumulator => _logArray.addAll(o.value)
    }
  }

  override def value: java.util.Set[String] = java.util.Collections.unmodifiableSet(_logArray)
}
