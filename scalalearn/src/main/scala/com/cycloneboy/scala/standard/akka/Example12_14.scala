package com.cycloneboy.scala.standard.akka

import akka.actor.SupervisorStrategy.{Escalate, Restart, Resume, Stop}
import akka.actor.{Actor, ActorLogging, ActorSystem, OneForOneStrategy, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 *
 * Create by  sl on 2019-11-14 09:36
 * <br/> Actor 的4中容错机制:重启\恢复\停止\上报
 */
object Example12_14 extends App {

  case class NormalMessage()

  class ChildActor extends Actor with ActorLogging {

    var state: Int = 0

    override def preStart(): Unit = {
      log.info("启动ChildActor,其 hashcode为:" + this.hashCode())
    }

    override def postStop(): Unit = {
      log.info("停止ChildActor,其 hashcode为:" + this.hashCode())
    }

    def receive = {
      case value: Int => {
        if (value <= 0)
          throw new ArithmeticException("数字小于等于0")
        else
          state = value
      }

      case result: NormalMessage => sender ! state
      case ex: NullPointerException => throw new NullPointerException("空指针")
      case _ => throw new IllegalArgumentException("非法参数")
    }
  }


  class SupervisorActor extends Actor with ActorLogging {

    val childActor = context.actorOf(Props[ChildActor], name = "ChildActor")

    override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 10 seconds) {
      case _: ArithmeticException => Resume
      case _: NullPointerException => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception => Escalate
    }

    def receive = {
      case msg: NormalMessage =>
        childActor.tell(msg, sender)
      case msg: Object =>
        childActor ! msg
    }

  }

  val system = ActorSystem("FaultToleranceSystem")
  val log = system.log

  val supervisor = system.actorOf(Props[SupervisorActor], name = "SupervisorActor")

  supervisor ! 5

  implicit val timeout = Timeout(5 seconds)
  var future = supervisor.ask(new NormalMessage)
  var resultMsg = Await.result(future, timeout.duration)
  log.info("结果:" + resultMsg)

  supervisor ! -5
  future = supervisor.ask(new NormalMessage)
  resultMsg = Await.result(future, timeout.duration)
  log.info("结果:" + resultMsg)


  supervisor ! new NullPointerException
  future = supervisor.ask(new NormalMessage)
  resultMsg = Await.result(future, timeout.duration)
  log.info("结果:" + resultMsg)

  supervisor.ask("字符串")

  system.stop(supervisor)

}
