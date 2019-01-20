package com.cycloneboy.scala.spark.algorithm

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * create by CycloneBoy on 2019-01-20 11:13
  *
  * PageRank 算法步骤
  * (1) 将每个页面的排序值初始化为1.0。
  * (2) 在每次迭代中，对页面p，向其每个相邻页面（有直接链接的页面）发送一个值为
  * rank(p)/numNeighbors(p) 的贡献值。
  * (3) 将每个页面的排序值设为0.15 + 0.85 * contributionsReceived。
  * 最后两步会重复几个循环，在此过程中，算法会逐渐收敛于每个页面的实际PageRank 值。
  * 在实际操作中，收敛通常需要大约10 轮迭代。
  */
object PageRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("PageRank")
    val sc = new SparkContext(conf)

    // 假设相邻页面列表以Spark objectFile的形式存储
    val links = sc.objectFile[(String, Seq[String])]("links")
      .partitionBy(new HashPartitioner(100))
      .persist()
    // 将每个页面的排序值初始化为1.0；由于使用mapValues，生成的RDD
    // 的分区方式会和"links"的一样
    var ranks = links.mapValues(v => 1.0)
    // 运行10轮PageRank迭代
    for(i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) =>
          links.map(dest => (dest, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
    }
    // 写出最终排名
    ranks.saveAsTextFile("ranks")
  }
}
