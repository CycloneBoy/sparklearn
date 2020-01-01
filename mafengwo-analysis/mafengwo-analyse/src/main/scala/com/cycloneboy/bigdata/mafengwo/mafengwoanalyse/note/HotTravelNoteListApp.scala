package com.cycloneboy.bigdata.mafengwo.mafengwoanalyse.note

import java.util.UUID

import com.cycloneboy.bigdata.mafengwo.mafengwoanalyse.utils.ProcessUtils
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.common.Constants
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.conf.ConfigurationManager
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.model.TravelHotNote
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.utils.DateUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 热门目的地的热门游记分析
 * Create by  sl on 2020-01-01 11:54
 */
object HotTravelNoteListApp {

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
    val hotTravelNoteListRdd: RDD[TravelHotNote] = ProcessUtils.getHotTravelNoteListRDD(spark, taskParam)
    hotTravelNoteListRdd.persist()

    println("蜂首游记总数: " + hotTravelNoteListRdd.count())

    ProcessUtils.printRDD(hotTravelNoteListRdd)

    val hotTravelRdd: RDD[(Long, (Long, String, String, String))] = hotTravelNoteListRdd.map {
      case (hotNote: TravelHotNote) => (hotNote.total_number, (hotNote.id, hotNote.country_name, hotNote.city_name, hotNote.city_url))
    }

    val sortHotTravelNoteListRdd: RDD[(Long, (Long, String, String, String))] = hotTravelRdd.sortByKey(false)
    sortHotTravelNoteListRdd.persist()

    println("------------------------------获取热门游记目的地前100的热门游记信息---------------------------------")
    sortHotTravelNoteListRdd.take(100) foreach println

    println("蜂首游记热门作者总数:" + sortHotTravelNoteListRdd.count())
    println("蜂首游记目的地:" + hotTravelNoteListRdd.count())

    // 关闭Spark上下文
    spark.close()
  }
}
