package com.cycloneboy.scala.spark.rdd

import org.apache.spark.rdd.RDD

/**
 *
 * Create by  sl on 2019-11-18 09:51
 */
class Search(query: String) extends Serializable {
  //  var : String = null

  /**
   * 过滤出包含字符串的数据
   */
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  /**
   * 过滤出包含字符串的RDD
   *
   * @param rdd
   * @return
   */
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  /**
   * 过滤出包含字符串的RDD
   *
   * @param rdd
   * @return
   */
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    val query_ : String = this.query
    rdd.filter(x => x.contains(query_))
  }

}
