package com.cycloneboy.scala.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 *
 * Create by  sl on 2019-11-20 21:51
 */
class CustomerReceiver(var host: String, var port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def receive() = {

    var socket = new Socket(host, port)

    var input: String = null
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
    input = reader.readLine()

    while (!isStopped() && input != null) {
      store(input)
      input = reader.readLine()
    }

    reader.close()
    socket.close()

    restart("restart")

  }

  override def onStop(): Unit = {}
}
