package com.cycloneboy.scala.standard.akka

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging

/**
 *
 * Create by  sl on 2019-11-13 20:12
 */
object Example12_6 extends App {

  class StringActor extends Actor {
    val log = Logging(context.system, this)

    override def preStart(): Unit = {
      log.info("preStart method in StringActor")
    }

    override def postStop(): Unit = {
      log.info("postStop method in StringActor")
    }

    override def unhandled(message: Any): Unit = {
      log.info("unhandled method in StringActor")
      super.unhandled(message)
    }

    def receive = {
      case s: String => log.info("receive message:\n" + s)
    }

  }

  val system = ActorSystem("StringSytem")

  // 使用默认的构造函数创建Actor实例
  val stringActor = system.actorOf(Props[StringActor], name = "StringActor")


  // 给stringActor 发送字符串消息
  stringActor ! "Creating Actors with default constructor"

  stringActor ! 123

  // 关闭ActorSytem
  //  system.stop()
  Thread.sleep(5000)

  system.stop(stringActor)
  println("end...")
}
