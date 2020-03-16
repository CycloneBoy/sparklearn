package com.cycloneboy.scala.standard.learnquick


import java.io.File

import scala.io.{BufferedSource, Source}
import scala.sys.process._

/**
 *
 * Create by  sl on 2020-01-21 22:19
 */
object FileLearn {

  def main(args: Array[String]): Unit = {

    val source: BufferedSource = Source.fromFile("/home/sl/workspace/java/a2019/sparklearn/scalalearn/src/main/resources/application.conf", "utf-8")
    val lines: Iterator[String] = source.getLines()
    val contents: String = source.mkString
    println(contents)


    //    "ls -al .." !
    "ls -al .." #> new File("output.txt")
    //    val result = "ls -al .." !!
    //      println(result)
    val properties: scala.collection.mutable.Map[String, String] = System.getProperties
    val value: AnyRef = properties.get("test")
    println(value)

    for (i <- (0 until 100).par) print(i + " ")
    for (i <- (0 until 100).par) yield (i + " ")
  }
}
