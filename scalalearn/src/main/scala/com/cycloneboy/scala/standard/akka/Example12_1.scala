package com.cycloneboy.scala.standard.akka

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging

/**
 *
 * Create by  sl on 2019-11-12 22:06
 * <br/>   使用默认的构造函数创建Actor实例
 */
object Example12_1 extends App {


  class StringActor extends Actor {

    val log = Logging(context.system, this)

    def receive = {
      case s: String => log.info("receive message:" + s)
      case _ => log.info("received unknown message")
    }

  }

  val system = ActorSystem("StringSytem")

  // 使用默认的构造函数创建Actor实例
  val stringActor = system.actorOf(Props[StringActor], name = "StringActor")


  // 给stringActor 发送字符串消息
  stringActor ! "Creating Actors with default constructor"

  // 关闭ActorSytem
  //  system.stop()
  Thread.sleep(5000)
  system.stop(stringActor)
}
