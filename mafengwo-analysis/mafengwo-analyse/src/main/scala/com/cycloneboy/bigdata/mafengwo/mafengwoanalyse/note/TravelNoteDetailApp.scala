package com.cycloneboy.bigdata.mafengwo.mafengwoanalyse.note

import java.util.UUID

import com.cycloneboy.bigdata.mafengwo.mafengwoanalyse.utils.ProcessUtils
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.common.Constants
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.conf.ConfigurationManager
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.model.TravelNote
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.utils.DateUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
 * 蜂首游记2010-2019年 总共 2189篇蜂首游记分析
 * Create by  sl on 2019-12-31 21:44
 */
object TravelNoteDetailApp {

  def main(args: Array[String]): Unit = {
    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)


    val taskUUID = DateUtils.getTodayStandard() + "_" + UUID.randomUUID().toString.replace("-", "")

    val sparkConf = new SparkConf().setAppName("TravelNoteDetailApp").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("error")

    val sc = spark.sparkContext

    // 导入蜂首热门游记
    val travelNoteRdd: RDD[TravelNote] = ProcessUtils.getTravelNoteRDDByYearRange(spark, taskParam)
    travelNoteRdd.persist()

    println("蜂首游记总数: " + travelNoteRdd.count())

    ProcessUtils.printRDD(travelNoteRdd)

    val destinationRdd: RDD[(String, Long)] = travelNoteRdd.map {
      case (travelNote: TravelNote) => (travelNote.destination, 1L)
    }

    // 任务一: 获取蜂首游记前100的热门目的地
    // 计算每个地点的热门游记的总数
    var destinationCountRdd: RDD[(String, Long)] = destinationRdd.reduceByKey(_ + _)

    // 过滤游记地点为空的数据
    destinationCountRdd = destinationCountRdd.filter(!_._1.isEmpty)

    // 对热门游记地点的总数进行排序
    val sortDestiantionRdd: RDD[(Long, String)] = destinationCountRdd.map {
      case (destination: String, sum: Long) => (sum, destination)
    }.sortByKey(false)
    sortDestiantionRdd.persist()

    println("------------------------------获取蜂首游记前100的热门目的地---------------------------------")
    sortDestiantionRdd.take(100) foreach println

    println("蜂首游记热门目的地总数:" + sortDestiantionRdd.count())
    println("蜂首游记总数:" + travelNoteRdd.count())

    // 任务一: 结束

    // 任务二: 获取蜂首游记写的最多的作者
    var authorInfoRdd: RDD[((String, String), Long)] = travelNoteRdd.map {
      case (travelNote: TravelNote) => ((travelNote.author_name, travelNote.author_url), 1L)
    }

    // 过滤作者名字为空的数据
    authorInfoRdd = authorInfoRdd.filter(!_._1._1.isEmpty)

    val authorCountRdd: RDD[((String, String), Long)] = authorInfoRdd.reduceByKey(_ + _)

    // 对蜂首游记作者的总数进行排序
    val sortAuthorRdd: RDD[(Long, (String, String))] = authorCountRdd.map {
      case ((authorName: String, auhorUrl: String), sum: Long) => (sum, (authorName, Constants.MAENGWO_HOST + auhorUrl))
    }.sortByKey(false)
    sortAuthorRdd.persist()

    println("------------------------------获取蜂首游记前100的热门作者---------------------------------")
    sortAuthorRdd.take(100) foreach println

    println("蜂首游记热门作者总数:" + sortAuthorRdd.count())
    println("蜂首游记目的地:" + travelNoteRdd.count())


    // 关闭Spark上下文
    spark.close()
  }
}
