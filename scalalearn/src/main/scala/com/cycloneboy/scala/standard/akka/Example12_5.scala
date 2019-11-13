package com.cycloneboy.scala.standard.akka

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 *
 * Create by  sl on 2019-11-13 19:46
 * <br/>
 * 消息处理: fire-and-forget 和send-and-receive-future
 */
object Example12_5 extends App {

  case class BasicInfo(id: Int, val name: String, age: Int)

  case class InterestInfo(id: Int, val interest: String)

  case class Person(basicInfo: BasicInfo, interestInfo: InterestInfo)

  class BasicInfoActor extends Actor {
    val log = Logging(context.system, this)

    def receive = {
      case id: Int => log.info("id=" + id); sender ! BasicInfo(id, "John", 19)
      case _ => log.info("receive unknow message")
    }
  }

  class InterestInfoActor extends Actor {
    val log = Logging(context.system, this)

    def receive = {
      case id: Int => log.info("id=" + id); sender ! InterestInfo(id, "足球")
      case _ => log.info("receive unknow message")
    }
  }

  class PersonActor extends Actor {
    val log = Logging(context.system, this)

    def receive = {
      case person: Person => log.info("person=" + person)
      case _ => log.info("receive unknow message")
    }
  }

  class CombineActor extends Actor {

    implicit val timeout = Timeout(5 seconds)

    val basicInfoActor = context.actorOf(Props[BasicInfoActor], name = "BasicInfoActor")
    val interestInfoActor = context.actorOf(Props[InterestInfoActor], name = "InterestInfo")
    val personActor = context.actorOf(Props[PersonActor], name = "Personctor")


    def receive = {
      case id: Int =>
        val combineResult: Future[Person] =
          for {
            basicInfo <- ask(basicInfoActor, id).mapTo[BasicInfo]
            interestInfo <- ask(interestInfoActor, id).mapTo[InterestInfo]
          } yield Person(basicInfo, interestInfo)

        // 将Future结果发送给PersonActor
        pipe(combineResult).to(personActor)
    }
  }

  val _system = ActorSystem("Send-And-Receive-Future")
  val combineActor = _system.actorOf(Props[CombineActor], name = "CombineActor")

  combineActor ! 12345

  Thread.sleep(5000)
  println("end...")
  _system.stop(combineActor)
}
