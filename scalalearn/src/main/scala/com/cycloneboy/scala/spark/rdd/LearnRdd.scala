package com.cycloneboy.scala.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by CycloneBoy on 2019-01-20 09:59
 */
object LearnRdd {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("LearnRdd") //设置本程序名称

    //Spark程序的编写都是从SparkContext开始的
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println(result.collect().mkString(","))

    val lines = sc.parallelize(List("hello world", "hi"))
    val words = lines.flatMap(line => line.split(" "))
    words.first() // 返回"hello

    //  需求：创建一个1-10数组的RDD，将所有元素*2形成新的RDD
    println("---需求：创建一个1-10数组的RDD，将所有元素*2形成新的RDD---")
    val source1 = sc.parallelize(1 to 10).cache()
    source1.collect() foreach println

    val mapadd = source1.map(_ * 2)
    mapadd.collect foreach println

    val mapPartition = source1.mapPartitions(_.map(_ * 2))
    mapPartition.collect() foreach println

    val mapPartitionWithIndex = source1.mapPartitionsWithIndex((index, items) => items.map((index, _)))
    mapPartitionWithIndex.collect() foreach println

    //  需求：flatmap
    println("需求：flatmap")
    val flatMap1 = source1.flatMap(1 to _)
    flatMap1.collect foreach println

    // 开发指导：当内存空间较大的时候建议使用mapPartition()，以提高处理效率

    // 需求：创建一个4个分区的RDD，并将每个分区的数据放到一个数组
    println("需求：创建一个4个分区的RDD，并将每个分区的数据放到一个数组")
    val source2 = sc.parallelize(1 to 16, 4)
    val glom1 = source2.glom().collect()
    glom1 foreach {
      t => println(t.mkString(", "))
    }

    // 需求：创建一个RDD（1-10），从中选择放回和不放回抽样
    println("需求：创建一个RDD（1-10），从中选择放回和不放回抽样")
    val sample1 = source1.sample(true, 0.4, 2)
    sample1.collect() foreach println

    val sample2 = source1.sample(false, 0.4, 3)
    sample2.collect() foreach println

  }
}
