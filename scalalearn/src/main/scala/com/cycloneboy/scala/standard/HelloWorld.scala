package com.cycloneboy.scala.standard

object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello world")

    println( "muliplier(1) value = " +  multiplier(1) )
    println( "muliplier(2) value = " +  multiplier(2) )
  }

  var factor = 3
  val multiplier = (i:Int) => i * factor

  def test(args : Array[String]) : String = {

    return args.toString
  }

}
