package com.cycloneboy.bigdata.businessanalysis.analyse.session

import java.util.{Date, UUID}

import com.cycloneboy.bigdata.businessanalysis.analyse.model._
import com.cycloneboy.bigdata.businessanalysis.analyse.utils.DataUtils
import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.conf.ConfigurationManager
import com.cycloneboy.bigdata.businessanalysis.commons.model.{UserInfo, UserVisitAction}
import com.cycloneboy.bigdata.businessanalysis.commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random


/**
 *
 * Create by  sl on 2019-11-26 12:17
 *
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 *
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 */

object UserVisitSessionAnalyze {

  //  val log = LoggerFactory.getLogger(UserVisitSessionAnalyze.getClass.toString)


  def main(args: Array[String]): Unit = {
    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)


    val taskUUID = DateUtils.getTodayStandard() + "_" + UUID.randomUUID().toString.replace("-", "")

    val sparkConf = new SparkConf().setAppName("UserVisitSessionAnalyze").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val sc = spark.sparkContext

    val actionRdd = getActionRDDByDateRange(spark, taskParam)

    // 打印测试数据是否读入正确
    println("-----------------打印测试数据是否读入正确-----------------------")
    actionRdd.take(5) foreach println

    // 将用户行为信息转换成 KV结构
    val sessionid2actionRDD: RDD[(String, UserVisitAction)] = actionRdd.map(action => (action.session_id, action))

    // 数据缓存
    sessionid2actionRDD.cache()

    // 将数据转换为Session粒度， 格式为<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
    val session2AggrInfoRDD: RDD[(String, SessionAggrInfo)] = aggregateBySession(spark, sessionid2actionRDD)

    // 打印session2AggrInfoRDD是否转换正确
    println("-----------------打印session2AggrInfoRDD是否转换正确-----------------------")
    session2AggrInfoRDD.take(5) foreach println

    // 设置自定义累加器，实现所有数据的统计功能,注意累加器也是懒执行的
    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator

    sc.register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator")

    // 根据查询任务的配置，过滤用户的行为数据，同时在过滤的过程中，对累加器中的数据进行统计
    // filteredSessionid2AggrInfoRDD是按照年龄、职业、城市范围、性别、搜索词、点击品类这些条件过滤后的最终结果

    val filteredSessionid2AggrInfoRDD: RDD[(String, SessionAggrInfo)] = filterSessionAndAggrStat(taskParam,
      session2AggrInfoRDD, sessionAggrStatAccumulator)
    println("--------------------------------根据查询任务的配置，过滤用户的行为数据，同时在过滤的过程中，对累加器中的数据进行统计----------------------------------------------")
    //    filteredSessionid2AggrInfoRDD.take(5) foreach println

    filteredSessionid2AggrInfoRDD.cache()

    // 打印filteredSessionid2AggrInfoRDD是否转换正确
    println("-----------------打印经过过滤关键词的session数据-----------------------")
    filteredSessionid2AggrInfoRDD.take(5) foreach println

    // sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
    // sessionid2detailRDD是原始完整数据与（用户 + 行为数据）聚合的结果，是符合过滤条件的完整数据
    // sessionid2detailRDD ( sessionId, userAction )
    val sessionid2detailRDD: RDD[(String, UserVisitAction)] = getSessionid2detailRDD(sessionid2actionRDD, filteredSessionid2AggrInfoRDD)

    sessionid2detailRDD.cache()
    println("-----------------打印代表了通过筛选的session对应的访问明细数据-----------------------")
    sessionid2detailRDD.take(5) foreach println

    // 业务功能一：统计各个范围的session占比，并写入MySQL
    calculateAndPersistAggrStat(spark, sessionAggrStatAccumulator.value, taskUUID)
    println("--------------------------------业务功能一：统计各个范围的session占比，并写入MySQL--------------------------------")

    // 业务功能二：随机均匀获取Session，之所以业务功能二先计算，是为了通过Action操作触发所有转换操作。
    randomExtractSession(taskUUID, spark, filteredSessionid2AggrInfoRDD, sessionid2detailRDD, taskParam)
    println("--------------------------------业务功能二：随机均匀获取Session，之所以业务功能二先计算，是为了通过Action操作触发所有转换操作。----------------------------------")

    // 业务功能三：获取top10热门品类
    // T返回排名前十的品类是为了在业务功能四中进行使用
    val top10CategoryRDD: RDD[Top10Category] = getTop10Category(taskUUID, spark, sessionid2detailRDD)

    println("--------------------------------业务功能三：获取top10热门品类----------------------------------")
    top10CategoryRDD.take(10) foreach println


    // 关闭Spark上下文
    spark.close()
  }

  /**
   * 业务需求三：获取top10热门品类
   *
   * @param taskUUID
   * @param spark
   * @param sessionid2detailRDD
   */
  def getTop10Category(taskUUID: String, spark: SparkSession, sessionid2detailRDD: RDD[(String, UserVisitAction)]) = {
    // 第一步：获取每一个Sessionid 点击过、下单过、支付过的数量

    // 获取所有产生过点击、下单、支付中任意行为的商品类别
    val categoryidRDD: RDD[(Long, Long)] = sessionid2detailRDD.flatMap { case (sessionid, userVisitAction: UserVisitAction) =>
      val list = ArrayBuffer[(Long, Long)]()

      // 一个session中点击的商品ID
      if (userVisitAction.click_category_id != -1L) {
        list += ((userVisitAction.click_category_id, userVisitAction.click_category_id))
      }

      // 一个session中下单的商品ID集合
      if (userVisitAction.order_category_ids != null) {
        for (orderCategoryId <- userVisitAction.order_category_ids.split(",")) {
          list += ((orderCategoryId.toLong, orderCategoryId.toLong))
        }
      }

      // 一个session中支付的商品ID集合
      if (userVisitAction.pay_category_ids != null) {
        for (payCategoryId <- userVisitAction.pay_category_ids.split(",")) {
          list += ((payCategoryId.toLong, payCategoryId.toLong))
        }
      }
      list
    }

    // 对重复的categoryid进行去重
    // 得到了所有被点击、下单、支付的商品的品类
    val distinctCategoryIdRDD: RDD[(Long, Long)] = categoryidRDD.distinct()


    // 第二步：计算各品类的点击、下单和支付的次数

    // 计算各个品类的点击次数
    val clickCategoryId2CountRDD: RDD[(Long, Long)] = getClickCategoryId2CountRDD(sessionid2detailRDD)
    // 计算各个品类的下单次数
    val orderCategoryId2CountRDD: RDD[(Long, Long)] = getOrderCategoryId2CountRDD(sessionid2detailRDD)
    // 计算各个品类的支付次数
    val payCategoryId2CountRDD: RDD[(Long, Long)] = getPayCategoryId2CountRDD(sessionid2detailRDD)

    // 第三步：join各品类与它的点击、下单和支付的次数
    // distinctCategoryIdRDD中是所有产生过点击、下单、支付行为的商品类别
    // 通过distinctCategoryIdRDD与各个统计数据的LeftJoin保证数据的完整性
    val categoryId2CountRDD: RDD[(Long, CategoryIdAggrInfo)] = joinCategoryAndData(distinctCategoryIdRDD, clickCategoryId2CountRDD,
      orderCategoryId2CountRDD, payCategoryId2CountRDD)

    // 第四步：自定义二次排序key

    // 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
    // 创建用于二次排序的联合key —— (CategorySortKey(clickCount, orderCount, payCount), line)
    // 按照：点击次数 -> 下单次数 -> 支付次数 这一顺序进行二次排序
    val sortKey2CountRDD: RDD[(CategorySortKey, CategoryIdAggrInfo)] = categoryId2CountRDD.map {
      case (categoryId, categoryIdAggrInfo: CategoryIdAggrInfo) =>
        (CategorySortKey(categoryIdAggrInfo.clickCategoryCount,
          categoryIdAggrInfo.orderCategoryCount, categoryIdAggrInfo.payCategoryCount), categoryIdAggrInfo)
    }

    // 降序排序
    val sortedCategoryCountRDD: RDD[(CategorySortKey, CategoryIdAggrInfo)] = sortKey2CountRDD.sortByKey(false)
    sortedCategoryCountRDD.cache()

    // 第六步：用take(10)取出top10热门品类，并写入MySQL

    val top10CategoryList: Array[(CategorySortKey, CategoryIdAggrInfo)] = sortedCategoryCountRDD.take(10)
    val top10Category: Array[Top10Category] = top10CategoryList.map { case (categorySortKey: CategorySortKey, categoryIdAggrInfo: CategoryIdAggrInfo) =>
      Top10Category(taskUUID, categoryIdAggrInfo.categoryId, categoryIdAggrInfo.clickCategoryCount,
        categoryIdAggrInfo.orderCategoryCount, categoryIdAggrInfo.payCategoryCount)
    }
    val top10CategoryRDD: RDD[Top10Category] = spark.sparkContext.makeRDD(top10Category)

    import spark.implicits._
    DataUtils.saveRDD2Mysql(top10CategoryRDD.toDS(), "top10_category")

    top10CategoryRDD
  }

  /**
   * 连接品类RDD与数据RDD
   *
   * @param categoryIdRDD
   * @param clickCategoryId2CountRDD
   * @param orderCategoryId2CountRDD
   * @param payCategoryId2CountRDD
   * @return
   */
  def joinCategoryAndData(categoryIdRDD: RDD[(Long, Long)],
                          clickCategoryId2CountRDD: RDD[(Long, Long)],
                          orderCategoryId2CountRDD: RDD[(Long, Long)],
                          payCategoryId2CountRDD: RDD[(Long, Long)]): RDD[(Long, CategoryIdAggrInfo)] = {
    // 将所有品类信息与点击次数信息结合【左连接】
    val clickJoidRDD: RDD[(Long, CategoryIdAggrInfo)] = categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD).map {
      case (categoryid, (cid, optionValue)) =>
        val clickCount = if (optionValue.isDefined) optionValue.get else 0L
        (categoryid, CategoryIdAggrInfo(categoryid, clickCount, 0L, 0L))
    }

    // 将所有品类信息与订单次数信息结合【左连接】
    val orderJoinRDD: RDD[(Long, CategoryIdAggrInfo)] = clickJoidRDD.leftOuterJoin(orderCategoryId2CountRDD).map {
      case (categoryid, (categoryIdAggrInfo, optionValue)) =>
        val orderCount = if (optionValue.isDefined) optionValue.get else 0L
        (categoryid, CategoryIdAggrInfo(categoryid, categoryIdAggrInfo.clickCategoryCount, orderCount, 0L))
    }

    val payJoinRDD: RDD[(Long, CategoryIdAggrInfo)] = orderJoinRDD.leftOuterJoin(orderCategoryId2CountRDD).map {
      case (categoryid, (categoryIdAggrInfo, optionValue)) =>
        val payCount = if (optionValue.isDefined) optionValue.get else 0L
        (categoryid, CategoryIdAggrInfo(categoryid, categoryIdAggrInfo.clickCategoryCount, categoryIdAggrInfo.orderCategoryCount, payCount))
    }
    payJoinRDD
  }

  /**
   * 获取各品类点击次数RDD
   *
   * @param sessionid2detailRDD
   */
  def getClickCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]) = {
    // 只将点击行为过滤出来
    val clickActionRDD: RDD[(String, UserVisitAction)] = sessionid2detailRDD.filter { case (sessionid, userVisitAction: UserVisitAction) =>
      userVisitAction.click_category_id != null
    }

    // 获取每种类别的点击次数
    // map阶段：(品类ID，1L)
    val clickCategoryIdRDD: RDD[(Long, Long)] = clickActionRDD.map { case (sessionid, userVisitAction: UserVisitAction) =>
      (userVisitAction.click_category_id, 1L)
    }

    // 计算各个品类的点击次数
    // reduce阶段：对map阶段的数据进行汇总
    // (品类ID1，次数) (品类ID2，次数) (品类ID3，次数) ... ... (品类ID4，次数)
    clickCategoryIdRDD.reduceByKey(_ + _)
  }

  /**
   * 获取各品类的下单次数RDD
   *
   * @param sessionid2detailRDD
   */
  def getOrderCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]) = {
    // 过滤订单数据
    val orderActionRDD: RDD[(String, UserVisitAction)] = sessionid2detailRDD.filter { case (sessionid, userVisitAction: UserVisitAction) =>
      userVisitAction.order_category_ids != null
    }

    // 获取每种类别的下单次数
    val orderCategoryIdRDD: RDD[(Long, Long)] = orderActionRDD.flatMap { case (sessionid, userVisitAction: UserVisitAction) =>
      userVisitAction.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }

    // 计算各个品类的下单次数
    orderCategoryIdRDD.reduceByKey(_ + _)


  }

  /**
   * 获取各品类的支付次数RDD
   *
   * @param sessionid2detailRDD
   */
  def getPayCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]) = {
    // 过滤支付数据
    val payActionRDD: RDD[(String, UserVisitAction)] = sessionid2detailRDD.filter { case (sessionid, userVisitAction: UserVisitAction) =>
      userVisitAction.pay_category_ids != null
    }

    // 获取每种类别的下单次数
    val payCategoryIdRDD: RDD[(Long, Long)] = payActionRDD.flatMap { case (sessionid, userVisitAction: UserVisitAction) =>
      userVisitAction.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }

    // 计算各个品类的下单次数
    payCategoryIdRDD.reduceByKey(_ + _)
  }

  /**
   * 业务需求二：随机抽取session
   *
   * @param taskUUID
   * @param spark
   * @param sessionid2AggrInfoRDD
   * @param sessionid2actionRDD
   */
  private def randomExtractSession(taskUUID: String, spark: SparkSession,
                                   sessionid2AggrInfoRDD: RDD[(String, SessionAggrInfo)],
                                   sessionid2actionRDD: RDD[(String, UserVisitAction)],
                                   taskParam: JSONObject) = {

    // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
    val time2sessionidRDD: RDD[(String, SessionAggrInfo)] = sessionid2AggrInfoRDD.map { case (sessionid, aggrInfo) =>
      val startTime = aggrInfo.startTime
      val dateHour = DateUtils.getDateHour(startTime)
      (dateHour, aggrInfo)
    }

    println("-----------------打印第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD-----------------------")
    time2sessionidRDD.take(5) foreach println

    // 得到每天每小时的session数量
    // countByKey()计算每个不同的key有多少个数据
    // countMap<yyyy-MM-dd_HH, count>
    val countMap: collection.Map[String, Long] = time2sessionidRDD.countByKey()

    // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引，将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
    // dateHourCountMap <yyyy-MM-dd,<HH,count>>
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()
    for ((dateHour, count) <- countMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      // 通过模式匹配实现了if的功能
      dateHourCountMap.get(date) match {
        // 对应日期的数据不存在，则新增
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long](); dateHourCountMap(date) += (hour -> count)
        // 对应日期的数据存在，则更新
        // 如果有值，Some(hourCountMap)将值取到了hourCountMap中
        case Some(hourCountMap) => hourCountMap += (hour -> count)
      }
    }

    // 按时间比例随机抽取算法，总共要抽取100个session，先按照天数，进行平分
    // 获取每一天要抽取的数量
    var extractNumber = ParamUtils.getParam(taskParam, Constants.PARAM_EXTRACT_SESSION_NUMBER)
    if (StringUtils.isEmpty(extractNumber)) {
      extractNumber = "100"
    }

    val extractNumberPerDay = extractNumber.toInt / dateHourCountMap.size

    // dateHourExtractMap[天，[小时，index列表]]
    val dateHourExtractMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()
    val random = new Random()

    /**
     * 根据每个小时应该抽取的数量，来产生随机值
     * 遍历每个小时，填充Map<date,<hour,(3,5,20,102)>>
     *
     * @param hourExtractMap
     * @param hourCountMap
     * @param sessionCountOfDay
     */
    def hourExtractMapFunc(hourExtractMap: mutable.HashMap[String, ListBuffer[Int]],
                           hourCountMap: mutable.HashMap[String, Long],
                           sessionCountOfDay: Long) = {

      for ((hour, count) <- hourCountMap) {
        // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
        // 就可以计算出，当前小时需要抽取的session数量
        var hourExtractNumber = ((count / sessionCountOfDay.toDouble) * extractNumberPerDay).toInt
        if (hourExtractNumber > count) {
          hourExtractNumber = count.toInt
        }

        // 仍然通过模式匹配实现有则追加，无则新建
        hourExtractMap.get(hour) match {
          case None => hourExtractMap(hour) = new mutable.ListBuffer[Int]
            // 根据数量随机生成下标
            for (i <- 0 to hourExtractNumber) {
              var extractIndex = random.nextInt(count.toInt)
              // 一旦随机生成的index已经存在，重新获取，直到获取到之前没有的index
              while (hourExtractMap(hour).contains(extractIndex)) {
                extractIndex = random.nextInt(count.toInt)
              }
              hourExtractMap(hour) += extractIndex
            }
          case Some(extractIndexList) =>
            // 根据数量随机生成下标
            for (i <- 0 to hourExtractNumber) {
              var extractIndex = random.nextInt(count.toInt)
              // 一旦随机生成的index已经存在，重新获取，直到获取到之前没有的index
              while (hourExtractMap(hour).contains(extractIndex)) {
                extractIndex = random.nextInt(count.toInt)
              }
              hourExtractMap(hour) += extractIndex
            }
        }
      }
    }

    // session 随机抽取功能
    for ((date, hourCountMap) <- dateHourCountMap) {
      // 计算出这一天的session总数
      val sessionCountOfDay: Long = hourCountMap.values.sum

      dateHourExtractMap.get(date) match {
        case None => dateHourExtractMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          // 更新index
          hourExtractMapFunc(dateHourExtractMap(date), hourCountMap, sessionCountOfDay)
        case Some(hourExtractMap) => hourExtractMapFunc(hourExtractMap, hourCountMap, sessionCountOfDay)
      }
    }

    /* 至此，index获取完毕 */
    println("------------------session 随机抽取功能-------------------")
    //将Map进行广播
    val dateHourExtractMapBroadcast: Broadcast[mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]] =
      spark.sparkContext.broadcast(dateHourExtractMap)

    // time2sessionidRDD <yyyy-MM-dd_HH,aggrInfo>
    // 执行groupByKey算子，得到<yyyy-MM-dd_HH,(session aggrInfo)>

    val time2sessionsRDD: RDD[(String, Iterable[SessionAggrInfo])] = time2sessionidRDD.groupByKey()

    // 第三步：遍历每天每小时的session，然后根据随机索引进行抽取,我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
    val sessionRandomExtract: RDD[SessionRandomExtract] = time2sessionsRDD.flatMap { case (dateHour, items) =>
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      // 从广播变量中提取出数据
      val dateHourExtractMap = dateHourExtractMapBroadcast.value
      // 获取指定天对应的指定小时的indexList
      // 当前小时需要的index集合
      val extractIndexList: ListBuffer[Int] = dateHourExtractMap.get(date).get(hour)

      // index是在外部进行维护
      var index = 0
      val sessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()
      for (sessionAggrInfo <- items) {
        if (extractIndexList.contains(index)) {
          sessionRandomExtractArray += SessionRandomExtract(taskUUID, sessionAggrInfo.sessionid,
            sessionAggrInfo.startTime, sessionAggrInfo.searchKeywords, sessionAggrInfo.clickCategoryIds)
        }
        // index自增
        index += 1

      }
      sessionRandomExtractArray
    }
    println("-----------------打印第三步：遍历每天每小时的session，然后根据随机索引进行抽取-----------------------")
    sessionRandomExtract.take(5) foreach println


    /* 将抽取后的数据保存到MySQL */

    // 引入隐式转换，准备进行RDD向Dataframe的转换
    import spark.implicits._
    DataUtils.saveRDD2Mysql[SessionRandomExtract](sessionRandomExtract.toDS(), "session_random_extract")
    println("-------------------------将抽取后的数据保存到MySQL-------------------------------------")

    // 提取抽取出来的数据中的sessionId
    val extractSessionidsRDD: RDD[(String, String)] = sessionRandomExtract.map { item => (item.sessionid, item.sessionid) }

    // 第四步：获取抽取出来的session的明细数据
    // 根据sessionId与详细数据进行聚合
    val extractSessionDetailRDD: RDD[(String, (String, UserVisitAction))] = extractSessionidsRDD.join(sessionid2actionRDD)

    // 对extractSessionDetailRDD中的数据进行聚合，提炼有价值的明细数据
    val sessionDetailRDD: RDD[SessionDetail] = extractSessionDetailRDD.map { case (sid, (sessionId, userVisitAction: UserVisitAction)) =>
      SessionDetail(taskUUID, userVisitAction.user_id, userVisitAction.session_id,
        userVisitAction.page_id, userVisitAction.action_time, userVisitAction.search_keyword,
        userVisitAction.click_category_id, userVisitAction.click_product_id, userVisitAction.order_category_ids,
        userVisitAction.order_product_ids, userVisitAction.pay_category_ids, userVisitAction.pay_product_ids)
    }

    println("-----------------打印对extractSessionDetailRDD中的数据进行聚合，提炼有价值的明细数据-----------------------")
    sessionDetailRDD.take(5) foreach println


    import spark.implicits._
    DataUtils.saveRDD2Mysql[SessionDetail](sessionDetailRDD.toDS(), "session_detail")
    println("--------------------第四步：获取抽取出来的session的明细数据---------------------")

  }


  /**
   * 计算各session范围占比，并写入MySQL
   *
   * @param spark
   * @param value
   * @param taskUUID
   */
  private def calculateAndPersistAggrStat(spark: SparkSession, value: mutable.HashMap[String, Int], taskUUID: String) = {

    value.foreach {
      case (k, v) => println(s"$k -> " + v)
    }


    val session_count = value(Constants.SESSION_COUNT).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat = SessionAggrStat(taskUUID,
      session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    import spark.implicits._
    val sessionAggrStatRDD: RDD[SessionAggrStat] = spark.sparkContext.makeRDD(Array(sessionAggrStat))
    DataUtils.saveRDD2Mysql[SessionAggrStat](sessionAggrStatRDD.toDS(), "session_aggr_stat")

  }

  /**
   * 获取通过筛选条件的session的访问明细数据RDD
   *
   * @param sessionid2actionRDD
   * @param filteredSessionid2AggrInfoRDD
   * @return
   */
  private def getSessionid2detailRDD(sessionid2actionRDD: RDD[(String, UserVisitAction)],
                                     filteredSessionid2AggrInfoRDD: RDD[(String, SessionAggrInfo)]): RDD[(String, UserVisitAction)] = {
    filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD).map(item => (item._1, item._2._2))
  }

  /**
   * 业务需求一：过滤session数据，并进行聚合统计
   *
   * @param taskParam
   * @param session2AggrInfoRDD
   * @param sessionAggrStatAccumulator
   * @return
   */
  private def filterSessionAndAggrStat(taskParam: JSONObject,
                                       session2AggrInfoRDD: RDD[(String, SessionAggrInfo)],
                                       sessionAggrStatAccumulator: AccumulatorV2[String, mutable.HashMap[String, Int]]): RDD[(String, SessionAggrInfo)] = {
    // 获取查询任务中的配置
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    val filterSessionid2AggrInfoRDD = session2AggrInfoRDD.filter {
      case (sessionid, aggrInfo) =>
        // 接着，依次按照筛选条件进行过滤
        // 按照年龄范围进行过滤（startAge、endAge）
        var success = true
        if ((StringUtils.isNotEmpty(startAge) && StringUtils.isNotEmpty(endAge)) && (aggrInfo.age < startAge.toInt || aggrInfo.age > endAge.toInt)) {
          success = false
        }

        // 按照职业范围进行过滤（professionals）
        // 互联网,IT,软件
        // 互联网
        if (!ValidUtils.in(professionals, aggrInfo.professional)) {
          success = false
        }

        // 按照城市范围进行过滤（cities）
        // 北京,上海,广州,深圳
        // 成都
        if (!ValidUtils.in(cities, aggrInfo.city)) {
          success = false
        }

        // 按照性别进行过滤
        // 男/女
        // 男，女
        if (!ValidUtils.in(sex, aggrInfo.sex)) {
          success = false
        }

        // 按照搜索词进行过滤
        // 我们的session可能搜索了 火锅,蛋糕,烧烤
        // 我们的筛选条件可能是 火锅,串串香,iphone手机
        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
        // 任何一个搜索词相当，即通过
        if (!ValidUtils.in(keywords, aggrInfo.searchKeywords)) {
          success = false
        }

        // 按照点击品类id进行过滤
        if (!ValidUtils.in(categoryIds, aggrInfo.clickCategoryIds)) {
          success = false
        }

        if (success) {


          sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
          //          log.debug("sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)")
          //          println("sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)")

          // 计算访问时长范围
          def calculateVisitLength(visitLength: Long) {
            if (visitLength >= 1 && visitLength <= 3) {
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
            } else if (visitLength >= 4 && visitLength <= 6) {
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
            } else if (visitLength >= 7 && visitLength <= 9) {
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
            } else if (visitLength >= 10 && visitLength <= 30) {
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
            } else if (visitLength > 30 && visitLength <= 60) {
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
            } else if (visitLength > 60 && visitLength <= 180) {
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
            } else if (visitLength > 180 && visitLength <= 600) {
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
            } else if (visitLength > 600 && visitLength <= 1800) {
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
            } else if (visitLength > 1800) {
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m)
            }
          }

          // 计算访问步长范围
          def calculateStepLength(stepLength: Long) {
            if (stepLength >= 1 && stepLength <= 3) {
              sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
            } else if (stepLength >= 4 && stepLength <= 6) {
              sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
            } else if (stepLength >= 7 && stepLength <= 9) {
              sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
            } else if (stepLength >= 10 && stepLength <= 30) {
              sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
            } else if (stepLength > 30 && stepLength <= 60) {
              sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
            } else if (stepLength > 60) {
              sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)
            }
          }

          // 计算出session的访问时长和访问步长的范围，并进行相应的累加
          calculateVisitLength(aggrInfo.visitLength)
          calculateStepLength(aggrInfo.stepLength)
        }
        success
    }

    filterSessionid2AggrInfoRDD

  }

  /**
   * 对Session数据进行聚合
   * 聚合每个userid 的访问信息统计
   *
   * @param spark
   * @param sessionid2actionRDD
   * @return
   */
  def aggregateBySession(spark: SparkSession, sessionid2actionRDD: RDD[(String, UserVisitAction)]): RDD[(String, SessionAggrInfo)] = {
    // 将数据转换为Session粒度， 格式为<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
    val sessionid2ActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionid2actionRDD.groupByKey()

    // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来，<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
    val userid2PartAggrInfoRDD = sessionid2ActionRDD.map { case (sessionid, userVisitActions) =>

      var searchKeywordsBuffer: StringBuffer = new StringBuffer("")
      var clickCategoryBuffer: StringBuffer = new StringBuffer("")

      var userid = -1L

      // session的起始和结束时间
      var startTime: Date = null
      var endTime: Date = null

      // session的访问步长
      var stepLength = 0

      // 遍历session所有的访问行为
      userVisitActions.foreach { userVisitAction =>
        if (userid == -1L) {
          userid = userVisitAction.user_id
        }

        val searchKeyword = userVisitAction.search_keyword
        val clickCategoryId = userVisitAction.click_category_id

        // 实际上这里要对数据说明一下
        // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
        // 其实，只有搜索行为，是有searchKeyword字段的
        // 只有点击品类的行为，是有clickCategoryId字段的
        // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

        // 我们决定是否将搜索词或点击品类id拼接到字符串中去
        // 首先要满足：不能是null值
        // 其次，之前的字符串中还没有搜索词或者点击品类id

        if (StringUtils.isNotEmpty(searchKeyword)) {
          if (!searchKeywordsBuffer.toString.contains(searchKeyword)) {
            searchKeywordsBuffer.append(searchKeyword + ",")
          }
        }

        if (clickCategoryId != null && clickCategoryId != -1L) {
          if (!clickCategoryBuffer.toString.contains(clickCategoryId.toString)) {
            clickCategoryBuffer.append(clickCategoryId + ",")
          }
        }

        // 计算session开始和结束时间
        val actionTime: Date = DateUtils.parseTime(userVisitAction.action_time)

        if (startTime == null || actionTime.before(startTime)) {
          startTime = actionTime
        }

        if (endTime == null || actionTime.after(endTime)) {
          endTime = actionTime
        }

        // 计算session访问步长
        stepLength += 1
      }

      val searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString)
      val clickCategoryIds = StringUtils.trimComma(clickCategoryBuffer.toString)

      // 计算session 访问时长(秒)
      val visitLength = (endTime.getTime - startTime.getTime) / 1000

      // 聚合数据
      val partAggrInfo = SessionAggrInfo(sessionid, searchKeywords, clickCategoryIds, visitLength, stepLength,
        DateUtils.formatTime(startTime), userid, -1, "", "", "")

      (userid.toString, partAggrInfo)
    }

    // 和用户数据进行join
    // 查询所有用户数据，并映射成<userid,Row>的格式
    import spark.implicits._
    val userid2InfoRDD: RDD[(String, UserInfo)] = spark.sql("select * from user_info").as[UserInfo].rdd.map(item => (item.user_id.toString, item))

    // 将session粒度聚合数据，与用户信息进行join
    val userid2FullInfoRDD: RDD[(String, (SessionAggrInfo, UserInfo))] = userid2PartAggrInfoRDD.join(userid2InfoRDD)

    // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
    userid2FullInfoRDD.map { case (userid, (partAggrInfo, userInfo)) =>
      val sessionid = partAggrInfo.sessionid

      // 聚合数据
      val fullAggrInfo = SessionAggrInfo(partAggrInfo.sessionid,
        partAggrInfo.searchKeywords, partAggrInfo.clickCategoryIds, partAggrInfo.visitLength, partAggrInfo.stepLength, partAggrInfo.startTime,
        userid.toLong, userInfo.age, userInfo.professional, userInfo.city, userInfo.sex)

      (sessionid, fullAggrInfo)
    }
  }

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
}
