package com.cycloneboy.bigdata.businessanalysis.data.kafka

import java.util.UUID

import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.conf.ConfigurationManager
import com.cycloneboy.bigdata.businessanalysis.commons.model.{ProductInfo, UserInfo, UserVisitAction}
import com.cycloneboy.bigdata.businessanalysis.commons.utils.{DateUtils, StringUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 *
 * Create by  sl on 2019-11-25 16:18
 */
object MockDataOfBusinessOffline {

  val maxNumberOfCity = ConfigurationManager.config.getInt(Constants.MOCK_NUMBER_OF_CITY)
  val maxNumberOfAd = ConfigurationManager.config.getInt(Constants.MOCK_NUMBER_OF_AD)
  val maxNumberOfUser = ConfigurationManager.config.getInt(Constants.MOCK_NUMBER_OF_USER)
  val maxNumberOfProduct = ConfigurationManager.config.getInt(Constants.MOCK_NUMBER_OF_PRODUCT)
  val maxNumberOfCategory = ConfigurationManager.config.getInt(Constants.MOCK_NUMBER_OF_CATEGORY)


  /**
   * 将DataFrame插入到Hive表中
   *
   * @param spark     SparkSQL客户端
   * @param tableName 表名
   * @param dataDF    DataFrame
   */
  private def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame) = {
    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  /**
   * 模拟用户行为信息
   *
   * @return
   */
  private def mockUserVistActionData(): Array[UserVisitAction] = {

    val searchKeywords = Array("5G", "猪肉", "大闸蟹", "玉米油", "羽绒服", "运动鞋", "口红", "华为手机", "笔记本,", "小龙虾", "卫生纸", "取暖", "Lamer", "机器学习", "苹果", "洗面奶", "保温杯")
    // yyyy-MM-dd
    val date = DateUtils.getTodayDate()
    // 关注四个行为：搜索、点击、下单、支付
    val actions = Array("search", "click", "order", "pay")

    val random = new Random()
    val rows = ArrayBuffer[UserVisitAction]()

    // 生成100个用户
    for (i <- 0 to maxNumberOfUser) {
      val userId = random.nextInt(100)
      // 每个用户生成10个session
      for (j <- 0 to 10) {
        val sessionId = UUID.randomUUID().toString.replace("-", "")
        // 在yyyy-MM-dd后面添加一个随机的小时时间（0-23）
        val baseActionTime = date + " " + random.nextInt(23)

        // 每个(userid + sessionid)生成0-100条用户访问数据
        for (k <- 0 to random.nextInt(100)) {
          val pageId = random.nextInt(10)

          // 在yyyy-MM-dd HH后面添加一个随机的分钟时间和秒时间
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var searchKeyword: String = null
          var clickCategoryId: Long = -1L
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityId = random.nextInt(maxNumberOfCity).toLong
          // 随机确定用户在当前session中的行为
          val action = actions(random.nextInt(4))

          action match {
            case "search" => searchKeyword = searchKeywords(random.nextInt(searchKeywords.length))
            case "click" => clickCategoryId = random.nextInt(maxNumberOfCategory).toLong
              clickProductId = random.nextInt(maxNumberOfProduct).toLong
            case "order" => orderCategoryIds = random.nextInt(maxNumberOfCategory).toString
              orderProductIds = random.nextInt(maxNumberOfProduct).toString
            case "pay" => payCategoryIds = random.nextInt(maxNumberOfCategory).toString
              payProductIds = random.nextInt(maxNumberOfProduct).toString
          }

          rows += UserVisitAction(date, userId, sessionId,
            pageId, actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds, cityId)
        }
      }
    }

    rows.toArray
  }

  /**
   * 模拟用户信息表
   *
   * @return
   */
  private def mockUserInfo(): Array[UserInfo] = {
    val rows = ArrayBuffer[UserInfo]()
    val sexes = Array("male", "female")
    val random = new Random()

    for (i <- 0 to maxNumberOfUser) {
      val userId = i
      val username = "user" + i
      val name = "name" + i
      val age = random.nextInt(60) + 10
      val professional = "professional" + random.nextInt(100)
      val city = "city" + random.nextInt(maxNumberOfCity)
      val sex = sexes(random.nextInt(2))
      rows += UserInfo(userId, username, name, age,
        professional, city, sex)
    }

    rows.toArray
  }

  /**
   * 模拟产品数据表
   *
   * @return
   */
  private def mockProductInfo(): Array[ProductInfo] = {
    val rows = ArrayBuffer[ProductInfo]()
    val random = new Random()
    // 自营或者第三方商品
    val productStatus = Array(0, 1)

    // 随机产生100个产品信息
    for (i <- 0 to maxNumberOfProduct) {
      val productId = i
      val productName = "product" + i
      val extendInfo = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}"

      rows += ProductInfo(productId, productName, extendInfo)
    }

    rows.toArray
  }

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("MockDataOfBusinessOffline")
      //      .config("spark.sql.warehouse.dir", "hdfs://localhost/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val userVisitActionData = this.mockUserVistActionData()
    val userInfoData = this.mockUserInfo()
    val productInfoData = this.mockProductInfo()

    // 测试数据
    println("----------------测试数据-------------------")
    println(userVisitActionData(0))
    println(userInfoData(0))
    println(productInfoData(0))
    println("----------------测试数据-------------------")

    val userVisitActionRdd = sparkSession.sparkContext.makeRDD(userVisitActionData)
    val userInfoRdd = sparkSession.sparkContext.makeRDD(userInfoData)
    val productInfoRdd = sparkSession.sparkContext.makeRDD(productInfoData)

    import sparkSession.implicits._
    // 将用户访问数据装换为DF保存到Hive表中
    val userVisitActionDF = userVisitActionRdd.toDF()
    insertHive(sparkSession, Constants.USER_VISIT_ACTION_TABLE, userVisitActionDF)

    // 将用户信息数据转换为DF保存到Hive表中
    val userInfoDF = userInfoRdd.toDF()
    insertHive(sparkSession, Constants.USER_INFO_TABLE, userInfoDF)

    // 将产品信息数据转换为DF保存到Hive表中
    val productInfoDF = productInfoRdd.toDF()
    insertHive(sparkSession, Constants.PRODUCT_INFO_TABLE, productInfoDF)


    sparkSession.close()
  }
}
