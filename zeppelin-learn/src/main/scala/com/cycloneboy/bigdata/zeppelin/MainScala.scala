package com.cycloneboy.bigdata.zeppelin

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * Create by  sl on 2020-08-02 11:05
 */
object MainScala {


  def main(args: Array[String]): Unit = {

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("spark://sl:7077")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()


    import spark.sql

    val frame: DataFrame = sql("show databases")

    frame.show()

  }
}
