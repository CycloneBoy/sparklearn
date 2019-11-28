package com.cycloneboy.bigdata.businessanalysis.analyse.utils

import com.cycloneboy.bigdata.businessanalysis.analyse.model.{CityId2CityInfo, CityIdProduct}
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


  /**
   * 根据日期获取对象的用户行为数据
   *
   * @param spark
   * @param taskParam
   * @return
   */
  def getCityId2ClickActionRDDByDateRange(spark: SparkSession, taskParam: JSONObject): RDD[CityIdProduct] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    import spark.implicits._
    val sql =
      "SELECT " +
        "city_id," +
        "click_product_id " +
        "FROM user_visit_action " +
        "WHERE click_product_id IS NOT NULL and click_product_id != -1L " +
        "AND date>='" + startDate + "' " +
        "AND date<='" + endDate + "'"

    spark.sql(sql).as[CityIdProduct].rdd

  }

  /**
   * 使用Spark SQL从MySQL中查询城市信息
   *
   * @param spark
   * @return
   */
  def getCityId2CityInfoRDD(spark: SparkSession): RDD[(Long, CityId2CityInfo)] = {
    val cityInfo = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"), (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"), (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"), (9L, "哈尔滨", "东北"))
    import spark.implicits._
    val cityInfoRDD = spark.sparkContext.makeRDD(cityInfo).toDF("city_id", "city_name", "area").as[CityId2CityInfo]

    cityInfoRDD.map(item => (item.city_id, item)).rdd
  }

}
