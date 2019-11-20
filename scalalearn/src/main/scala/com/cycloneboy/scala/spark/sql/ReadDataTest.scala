package com.cycloneboy.scala.spark.sql

import org.apache.spark.sql.SparkSession

/**
 *
 * Create by  sl on 2019-11-20 12:06
 */
object ReadDataTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("ReadDataTest").getOrCreate()

    // Spark SQL 能够自动推测 JSON数据集的结构，并将它加载为一个Dataset[Row].
    // 可以通过SparkSession.read.json()去加载一个 一个JSON 文件。
    println("----------------------JSON格式文件读取------------------------------")
    val peopleDF = spark.read.json("hdfs://localhost:9000/user/sl/spark/salary.json").cache()
    peopleDF.show()
    peopleDF.printSchema()

    peopleDF.createOrReplaceTempView("people")

    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE salary BETWEEN 1000 AND 2600")
    teenagerNamesDF.show()

    import spark.implicits._
    val otherPeopleDataset = spark.createDataset(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()
    otherPeople.printSchema()

    // Parquet是一种流行的列式存储格式，可以高效地存储具有嵌套字段的记录。
    // Parquet格式经常在Hadoop生态圈中被使用，它也支持Spark SQL的全部数据类型
    println("----------------------Parquet格式文件读取------------------------------")
    peopleDF.write.parquet("hdfs://localhost:9000/user/sl/spark/people.parquet")

    val parquetFileDF = spark.read.parquet("hdfs://localhost:9000/user/sl/spark/people.parquet")

    parquetFileDF.createOrReplaceTempView("parquetFile")

    val namesDF = spark.sql("SELECT name, salary FROM parquetFile WHERE salary BETWEEN 1000 AND 2600")
    namesDF.map(attributes => "Name: " + attributes(0) + " Salary: " + attributes(1)).show()
  }
}
