package com.cycloneboy.bigdata.flink.base

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

/**
 *
 * Create by  sl on 2020-03-30 21:53
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val input = "file:///home/sl/workspace/bigdata/flink.txt"

    val ds: DataSet[String] = env.readTextFile(input)

    import org.apache.flink.api.scala.createTypeInformation
    val aggDs: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    aggDs.print()

  }
}
