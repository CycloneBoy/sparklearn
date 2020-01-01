package com.cycloneboy.bigdata.mafengwo.analyse

import java.util.UUID

import com.cycloneboy.bigdata.mafengwo.mafengwocommon.common.Constants
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.conf.ConfigurationManager
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.model.TravelNote
import com.cycloneboy.bigdata.mafengwo.mafengwocommon.utils.DateUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 *
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
    //    spark.sparkContext.setLogLevel("error")

    val sc = spark.sparkContext


    //    val startYear = ParamUtils.getParam(taskParam, Constants.PARAM_START_YEAR)
    //    val endYear = ParamUtils.getParam(taskParam, Constants.PARAM_END_YEAR)
    //    val travelNoteRdd = spark.sql("select * from t_travel_note_hive")
    import spark.implicits._
    val rdd: RDD[TravelNote] = spark.sql("select * from t_travel_note_hive")
      .as[TravelNote].rdd

    rdd.take(5) foreach println
    //    ProcessUtils.printRDD(travelNoteRdd)


    // 关闭Spark上下文
    spark.close()
  }
}
