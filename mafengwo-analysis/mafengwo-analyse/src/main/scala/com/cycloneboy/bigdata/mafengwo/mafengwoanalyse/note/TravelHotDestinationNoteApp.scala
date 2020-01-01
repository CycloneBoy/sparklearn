package com.cycloneboy.bigdata.mafengwo.mafengwoanalyse.note

import java.util.UUID

import com.cycloneboy.bigdata.mafengwo.mafengwoanalyse.utils.ProcessUtils
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.common.Constants
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.conf.ConfigurationManager
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.model.TravelHotNoteDetail
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.utils.{DateUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 *
 * Create by  sl on 2020-01-01 12:39
 */
object TravelHotDestinationNoteApp {

  def main(args: Array[String]): Unit = {
    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)


    val taskUUID = DateUtils.getTodayStandard() + "_" + UUID.randomUUID().toString.replace("-", "")

    val sparkConf = new SparkConf().setAppName("HotTravelNoteListApp").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("error")

    val sc = spark.sparkContext

    // 导入热门游记目的地的热门游记列表
    var travelHotDestinationNoteRdd: RDD[TravelHotNoteDetail] = ProcessUtils.getTravelHotDstinationNoteRDD(spark, taskParam)
    println("热门游记总数: " + travelHotDestinationNoteRdd.count())


    calcHotNote()

    // 关闭Spark上下文
    spark.close()
  }

  /**
   * 根据游记的阅读数,点赞数和评论数计算统一排名
   * 1:5:10
   *
   * @param note TravelHotNoteDetail
   * @return
   */
  def calcNoteRank(note: TravelHotNoteDetail): Long = {

    var result = note.travel_view_count.toLong + note.travel_up_count.toLong * 5 + note.travel_comment_count.toLong * 10
    result

  }

  /**
   * 第一个分析任务: 找出浏览量最大的100篇游记
   */
  def calcHotNote: Unit = {
    // 对热门游记目的地进行过滤,过滤掉 阅读排序字段 数据为空的数字段
    travelHotDestinationNoteRdd = travelHotDestinationNoteRdd.filter {
      note: TravelHotNoteDetail =>
        if (note != null
          && StringUtils.isNotEmpty(note.travel_view_count)
          && StringUtils.isNotEmpty(note.travel_up_count)
          && StringUtils.isNotEmpty(note.travel_comment_count)) {
          try {
            note.travel_view_count.toLong
            note.travel_up_count.toLong
            note.travel_comment_count.toLong
            true
          }
          catch {
            case e: NumberFormatException => {
              false
            }
          }
        } else false
    }

    // 游记去重
    travelHotDestinationNoteRdd.distinct()
    travelHotDestinationNoteRdd.persist(StorageLevel.DISK_ONLY)

    println("热门游记总数: " + travelHotDestinationNoteRdd.count())

    ProcessUtils.printRDD(travelHotDestinationNoteRdd)

    // 对热门游记进行按浏览量,点赞量和评论量进行排序
    val hotTravelRdd: RDD[(Long, TravelHotNoteDetail)] = travelHotDestinationNoteRdd.map {
      case note: TravelHotNoteDetail => {
        (note.travel_view_count.toLong + note.travel_up_count.toLong * 5 + note.travel_comment_count.toLong * 10, note)
      }
    }

    // 根据浏览量进行游记排序
    val sortHotTravelRdd: RDD[(Long, TravelHotNoteDetail)] = hotTravelRdd.sortByKey(false)

    val travelIdAndSumRdd: RDD[(String, (Long, TravelHotNoteDetail))] = sortHotTravelRdd.map {
      case (sum: Long, note: TravelHotNoteDetail) => (note.travel_id, (sum, note))
    }

    // 根据travelID 进行去重
    val travelIdRdd: RDD[(String, (Long, TravelHotNoteDetail))] = travelIdAndSumRdd.reduceByKey {
      case ((sum1, note1), _) => (sum1, note1)
    }

    println("热门游记去重后的总数:" + travelIdRdd.count())
    println("热门游记去重前的总数:" + travelIdAndSumRdd.count())

    val sortTravelHotNoteRdd: RDD[(Long, (String, TravelHotNoteDetail))] = travelIdRdd.map {
      case (id, (sum, note)) => (sum, (id, note))
    }

    // 获取最热门的100篇游记
    println("--------------------------获取最热门的100篇游记----------------------------------")
    sortTravelHotNoteRdd.take(100) foreach println

    println("热门游记热门作者总数:" + sortTravelHotNoteRdd.count())
    println("热门游记目的地:" + travelHotDestinationNoteRdd.count())
  }

}
