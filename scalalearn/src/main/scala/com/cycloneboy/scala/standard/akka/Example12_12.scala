package com.cycloneboy.scala.standard.akka

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.dispatch.{PriorityGenerator, UnboundedPriorityMailbox}
import akka.event.{Logging, LoggingAdapter}
import com.typesafe.config.{Config, ConfigFactory}

/**
 *
 * Create by  sl on 2019-11-14 09:06
 * <br/> dispatcher 调度
 * <br/> BalancingDispatcher 按照消息的优先级进行处理,高优先级的先处理
 *
 */
object Example12_12 extends App {

  class MyPrioMailBox(settings: ActorSystem.Settings, config: Config) extends UnboundedPriorityMailbox(

    PriorityGenerator {
      case 'highpriority => 0
      case 'lowpriority => 2
      case PoisonPill => 3
      case otherwise => 1
    }
  )


  val _system = ActorSystem.create("DsipatcherSystem", ConfigFactory.load().getConfig("MyDispatcherExample"))


  // 使用默认的构造函数创建Actor实例
  val stringActor = _system.actorOf(Props(new Actor {

    val log: LoggingAdapter = Logging(context.system, this)

    self ! 'lowpriority
    self ! 'lowpriority
    self ! 'highpriority
    self ! 'pigdog
    self ! 'pigdog2
    self ! 'pigdog3
    self ! 'highpriority
    self ! PoisonPill

    def receive = {
      case x => log.info(x.toString)
    }

  }).withDispatcher("balancingDispatcher"), name = "UnboundedPriorityMailboxActor")


  // 关闭ActorSytem
  //  system.stop()
  Thread.sleep(5000)


}
