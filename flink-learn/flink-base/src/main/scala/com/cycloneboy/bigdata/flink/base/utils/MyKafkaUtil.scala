package com.cycloneboy.bigdata.flink.base.utils

import java.util.Properties

/**
 *
 * Create by  sl on 2020-03-31 11:56
 */
object MyKafkaUtil {

  val prop = new Properties()

  prop.setProperty("bootstrap.servers", "hadoop1:9092")
  prop.setProperty("group.id", "gmall")

  //  def getComsumer(topic:String) = {
  //    new FlinkKafkaConsumer011
  //  }
}
