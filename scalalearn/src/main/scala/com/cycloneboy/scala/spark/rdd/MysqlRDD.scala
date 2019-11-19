package com.cycloneboy.scala.spark.rdd

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Create by  sl on 2019-11-19 16:34
 *
 * 支持通过Java JDBC访问关系型数据库。需要通过JdbcRDD进行
 */
case class Student(id: Long, name: String)

object MysqlRDD {

  //3.定义连接mysql的参数
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/mysqlsource"
  val userName = "root"
  val passWd = "123456"

  /**
   * 获取mysql数据连接
   *
   * @return
   */
  def getConn = () => {
    Class.forName(driver)
    DriverManager.getConnection(url, userName, passWd)
  }

  def insertData(iterator: Iterator[String]): Unit = {
    iterator.foreach(data => {
      val ps = getConn().prepareStatement("insert into student(name) values (?)")
      ps.setString(1, data)
      ps.executeUpdate()
    })
  }

  def main(args: Array[String]): Unit = {

    //设置本程序名称
    val conf = new SparkConf().setMaster("local[*]").setAppName("MysqlRDD")

    //Spark程序的编写都是从SparkContext开始的
    val sc = new SparkContext(conf)


    //创建JdbcRDD 查询语句必须包含两个占位符
    val mysqlRdd = new JdbcRDD(sc, getConn, "select * from student where id >=? and id <=?;",
      1, 10, 1,
      r => Student(r.getInt(1), r.getString(2)))

    println(mysqlRdd.count())

    // 查询数据测试
    println("---------------查询数据测试------------")
    mysqlRdd.collect foreach println


    // 插入数据测试
    println("---------------插入数据测试------------")
    val data = sc.parallelize(List("Female", "Male", "Female"))
    data.foreachPartition(insertData)

    println("---------------查询数据测试------------")
    mysqlRdd.collect foreach println
    sc.stop()
  }


}
