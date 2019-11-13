package com.cycloneboy.scala.standard.akka

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging

/**
 *
 * Create by  sl on 2019-11-13 17:23
 */
object Example12_4 extends App {

  case class Start(var msg: String)

  case class Run(var msg: String)

  case class Stop(var msg: String)

  class ExampleActor extends Actor {

    val other = context.actorOf(Props[OtherActor], "OtherActor")
    val log = Logging(context.system, this)

    def receive = {
      case Start(msg) => other ! msg
      case Run(msg) => other.tell(msg, sender)
    }
  }

  class OtherActor extends Actor {
    val log = Logging(context.system, this)

    def receive = {
      case s: String => log.info("receive message:\n" + s)
      case _ => log.info("receive unknown message")
    }
  }


  val system = ActorSystem("MessageProcessingSytem")

  val exampleActor = system.actorOf(Props[ExampleActor], name = "ExampleActor")

  exampleActor ! Run("Running")
  exampleActor ! Start("Starting")

  Thread.sleep(5000)
  system.stop(exampleActor)
}
