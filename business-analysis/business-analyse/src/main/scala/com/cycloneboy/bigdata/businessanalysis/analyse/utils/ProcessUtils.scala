package com.cycloneboy.bigdata.businessanalysis.analyse.utils

import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.model.UserVisitAction
import com.cycloneboy.bigdata.businessanalysis.commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 *
 * Create by  sl on 2019-11-27 14:36
 */
object ProcessUtils {


  /**
   * 根据日期获取对象的用户行为数据
   *
   * @param spark
   * @param taskParam
   * @return
   */
  def getActionRDDByDateRange(spark: SparkSession, taskParam: JSONObject): RDD[UserVisitAction] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    import spark.implicits._
    spark.sql("select * from user_visit_action where date >='" + startDate + "' and date<='" + endDate + "'")
      .as[UserVisitAction].rdd

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


}
