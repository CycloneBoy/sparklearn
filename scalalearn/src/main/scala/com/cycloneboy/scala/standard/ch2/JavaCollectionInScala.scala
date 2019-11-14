package com.cycloneboy.scala.standard.ch2

import java.util

/**
 *
 * Create by  sl on 2019-11-14 11:00
 */
object JavaCollectionInScala extends App {

  def getList = {
    val list = new util.ArrayList[String]()
    list.add("Hadoop")
    list.add("Hive")

    list
  }

  val list = getList
  list.forEach(println)


}
