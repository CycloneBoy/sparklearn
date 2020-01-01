package com.cycloneboy.bigdata.mafengwo.mafengwoanalyse.utils

import com.cycloneboy.bigdata.mafengwo.mafengwocommon.common.Constants
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.model.{TravelHotNote, TravelHotNoteDetail, TravelNote}
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 *
 * Create by  sl on 2019-11-27 14:36
 */
object ProcessUtils {


  /**
   * 根据日期获取对象的蜂首游记数据
   *
   * @param spark
   * @param taskParam
   * @return
   */
  def getTravelNoteRDDByYearRange(spark: SparkSession, taskParam: JSONObject): RDD[TravelNote] = {
    val startYear = ParamUtils.getParam(taskParam, Constants.PARAM_START_YEAR)
    val endYear = ParamUtils.getParam(taskParam, Constants.PARAM_END_YEAR)
    import spark.implicits._
    spark.sql("select * from t_travel_note_hive where year >='" + startYear + "' and year<'" + endYear + "'")
      .as[TravelNote].rdd
  }

  /**
   * 打印RDD数据
   *
   * @param actionRdd    RDD
   * @param numberOfline 打印行数,默认5行
   */
  def printRDD[T](actionRdd: RDD[T], numberOfline: Int = 5): Unit = {
    // 打印测试数据是否读入正确
    println(s"-----------------打印测试数据是否读入正确-----------------------")
    actionRdd.take(numberOfline) foreach println
  }


  /**
   * 获取热门游记目的地的游记数据
   *
   * @param spark
   * @param taskParam
   * @return
   */
  def getHotTravelNoteListRDD(spark: SparkSession, taskParam: JSONObject): RDD[TravelHotNote] = {
    import spark.implicits._
    spark.sql("select * from t_hot_travel_note_list")
      .as[TravelHotNote].rdd
  }

  /**
   * 获取热门游记目的地的游记数据
   *
   * @param spark
   * @param taskParam
   * @return
   */
  def getTravelHotDstinationNoteRDD(spark: SparkSession, taskParam: JSONObject): RDD[TravelHotNoteDetail] = {
    import spark.implicits._
    spark.sql("select * from t_travel_hot_destination_note")
      .as[TravelHotNoteDetail].rdd
  }
}
