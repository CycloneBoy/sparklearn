package com.cycloneboy.scala.standard.ch2

/**
 *
 * Create by  sl on 2019-11-11 16:35
 */
object Example6_01 {

  object Student {

    private var studentNo: Int = 0

    def uniqueStudentNo() = {
      studentNo += 1
      studentNo
    }

  }

  def main(args: Array[String]): Unit = {
    println(Student.uniqueStudentNo())
  }
}
