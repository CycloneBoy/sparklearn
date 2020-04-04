package com.cycloneboy.bigdata.flink.sink

import com.cycloneboy.bigdata.flink.model.Alert
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory

/**
 *
 * Create by  sl on 2020-04-04 11:32
 */
class AlertSink extends SinkFunction[Alert] {

  private val LOG = LoggerFactory.getLogger(classOf[AlertSink])

  override def invoke(value: Alert, context: SinkFunction.Context[_]): Unit = {
    LOG.info(value.toString)
  }


}
