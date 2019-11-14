package com.cycloneboy.scala.standard.akka

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging
import akka.routing.RandomPool

/**
 *
 * Create by  sl on 2019-11-14 09:25
 * <br/> Router 进行路由转发
 */
object Example12_13 extends App {

  class IntActor extends Actor {

    val log = Logging(context.system, this)

    def receive = {
      case s: Int => log.info("received message:\n" + s)
      case _ => log.info("received unknowwn message")
    }
  }


  val _system = ActorSystem("RandomRouterExample")

  // 使用默认的构造函数创建Actor实例
  val randomRouter = _system.actorOf(Props[IntActor].withRouter(RandomPool(5)), name = "IntActor")


  1 to 10 foreach {
    i => randomRouter ! i
  }


  Thread.sleep(5000)
  _system.stop(randomRouter)

}
