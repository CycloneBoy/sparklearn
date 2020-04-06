package com.cycloneboy.bigdata.flink.base.chapter5.util

import org.apache.flink.api.common.functions.FilterFunction

/**
 *
 * Create by  sl on 2020-04-06 11:25
 */
class KeywordFilter(keyword: String) extends FilterFunction[String] {
  override def filter(value: String): Boolean = value.contains(keyword)
}
