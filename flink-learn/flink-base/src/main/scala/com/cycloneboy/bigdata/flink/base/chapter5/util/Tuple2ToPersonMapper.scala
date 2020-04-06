package com.cycloneboy.bigdata.flink.base.chapter5.util

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types

/**
 *
 * Create by  sl on 2020-04-06 11:15
 */
case class Person(name: String, age: Int)

class Tuple2ToPersonMapper extends MapFunction[(String, Int), Person] with ResultTypeQueryable[Person] {

  override def map(value: (String, Int)): Person = Person(value._1, value._2)

  override def getProducedType: TypeInformation[Person] = {
    Types.CASE_CLASS[Person]
  }
}
