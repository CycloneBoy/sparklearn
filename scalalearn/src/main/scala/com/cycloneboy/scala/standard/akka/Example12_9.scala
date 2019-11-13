package com.cycloneboy.scala.standard.akka

import akka.actor.TypedActor.{PostStop, PreStart}
import akka.actor.{ActorSystem, TypedActor, TypedProps}
import akka.event.Logging

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

/**
 *
 * Create by  sl on 2019-11-13 20:32
 *
 */
object Example12_9 extends App {

  trait Squarer {

    def squareDontCare(i: Int): Unit

    def square(i: Int): Future[Int]

    def squareNowPlease(i: Int): Option[Int]

    def squareNow(i: Int): Int
  }

  class SquarerImpl(val name: String) extends Squarer with PostStop with PreStart {

    val log = Logging(system, this.getClass)


    def this() = this("SquarerImpl")

    override def squareDontCare(i: Int): Unit = i * i

    override def square(i: Int): Future[Int] = Promise.successful(i * i).future

    override def squareNowPlease(i: Int): Option[Int] = Some(i * i)

    override def squareNow(i: Int): Int = i * i

    override def postStop(): Unit = {
      log.info("TypeActor stopped")
    }

    override def preStart(): Unit = {
      log.info("TypeActor started")
    }
  }


  val system = ActorSystem("StringSytem")

  val log = Logging(system, this.getClass)

  // 使用默认的构造函数创建Actor实例
  val mySquarer: Squarer = TypedActor(system).typedActorOf(TypedProps[SquarerImpl], name = "mySquarer")
  val otherSquarer: Squarer = TypedActor(system).typedActorOf(TypedProps(classOf[Squarer],
    new SquarerImpl("SquarerImpl")), name = "otherSquarer")

  mySquarer.squareDontCare(10)

  val oSquare = mySquarer.squareNowPlease(10)

  log.info("oSquare=" + oSquare)

  val iSquare = mySquarer.squareNow(10)
  log.info("iSquare=" + iSquare)

  val fSquare = mySquarer.square(10)

  val result = Await.result(fSquare, 5 seconds)
  log.info("fSquare=" + result)

  Thread.sleep(5000)

  TypedActor(system).poisonPill(otherSquarer)

  TypedActor(system).stop(mySquarer)


}
