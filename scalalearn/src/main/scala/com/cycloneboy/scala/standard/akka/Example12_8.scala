package com.cycloneboy.scala.standard.akka

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.event.Logging

/**
 *
 * Create by  sl on 2019-11-12 22:06
 * <br/> 停止Actor system.stop(contextActor)
 */
object Example12_8 extends App {


  class StringActor extends Actor {

    val log = Logging(context.system, this)

    def receive = {
      case s: String => log.info("receive message:" + s)
      case _ => log.info("received unknown message")
    }

    override def postStop(): Unit = {
      log.info("postStop method in StringActor")
    }
  }

  class ContextActor extends Actor {

    val log = Logging(context.system, this)

    // 通过context 创建StringActor
    var stringActor = context.actorOf(Props[StringActor], name = "stringActor3")

    def receive = {
      case s: String => {
        log.info("receive message:\n" + s)
        stringActor ! s
        context.stop(stringActor)
      }
      case _ => log.info("receive unknown message")
    }

    override def postStop(): Unit = {
      log.info("postStop method in ContextActor")
    }
  }

  val system = ActorSystem("StringSytem")

  val contextActor = system.actorOf(Props[ContextActor], name = "ContextActor")

  contextActor ! "Creating Actors with  val context"

  Thread.sleep(5000)

  contextActor ! PoisonPill
  //system.stop(contextActor)
}
