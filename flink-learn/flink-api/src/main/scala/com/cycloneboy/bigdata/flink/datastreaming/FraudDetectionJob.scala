package com.cycloneboy.bigdata.flink.datastreaming

import com.cycloneboy.bigdata.flink.model.{Alert, Transaction}
import com.cycloneboy.bigdata.flink.sink.AlertSink
import com.cycloneboy.bigdata.flink.source.TransactionSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
 *
 * Create by  sl on 2020-04-04 11:17
 */
object FraudDetectionJob {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val transactions: DataStream[Transaction] = env
      // SensorSource generates random temperature readings
      .addSource(new TransactionSource)
    // assign timestamps and watermarks which are required for event time
    //      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    transactions.print()

    val alerts: DataStream[Alert] = transactions
      .keyBy(transaction => transaction.accountId)
      .process(new FraudDetector)
      .name("fraud-detector")

    alerts.addSink(new AlertSink)

    env.execute("Fraud Detection")
  }
}
