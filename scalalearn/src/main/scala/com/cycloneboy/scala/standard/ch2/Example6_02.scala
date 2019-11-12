package com.cycloneboy.scala.standard.ch2

/**
 *
 * Create by  sl on 2019-11-11 16:35
 */
object Example6_02 extends App {

  class Student {

    var name: String = _

    def getStudentNo: Int = {
      Student.uniqueStudentNo()
      Student.studentNo
    }
  }

  object Student {

    var studentNo: Int = 0

    def uniqueStudentNo(): Int = {
      studentNo += 1
      studentNo
    }

    def printStudentName(): Unit = println(new Student().name)

    def apply(): Student = new Student()
  }


  println(Student.uniqueStudentNo())

  println(Student.printStudentName())
  println(new Student().getStudentNo)

  println(Student().getStudentNo)

  val s = Student()
  s.name = "Bob"

  println(s.name)
}
