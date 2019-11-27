package com.cycloneboy.bigdata.businessanalysis.analyse.page

import java.util.UUID

import com.cycloneboy.bigdata.businessanalysis.analyse.model.PageSplitConvertRate
import com.cycloneboy.bigdata.businessanalysis.analyse.utils.DataUtils
import com.cycloneboy.bigdata.businessanalysis.analyse.utils.ProcessUtils.{getActionRDDByDateRange, printRDD}
import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.conf.ConfigurationManager
import com.cycloneboy.bigdata.businessanalysis.commons.model.UserVisitAction
import com.cycloneboy.bigdata.businessanalysis.commons.utils.{DateUtils, NumberUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 *
 * Create by  sl on 2019-11-27 14:34
 *
 * 页面单跳转化率模块spark作业
 *
 * 页面转化率的求解思路是通过UserAction表获取一个session的所有UserAction，根据时间顺序排序后获取全部PageId
 * 然后将PageId组合成PageFlow，即1,2,3,4,5的形式（按照时间顺序排列），之后，组合为1_2, 2_3, 3_4, ...的形式
 * 然后筛选出出现在targetFlow中的所有A_B
 *
 * 对每个A_B进行数量统计，然后统计startPage的PV，之后根据targetFlow的A_B顺序，计算每一层的转化率
 *
 */
object PageOneStepConvertRate {

  def main(args: Array[String]): Unit = {

    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)


    val taskUUID = DateUtils.getTodayStandard() + "_" + UUID.randomUUID().toString.replace("-", "")

    val sparkConf = new SparkConf().setAppName("PageOneStepConvertRate").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("error")

    // 查询指定日期范围内的用户访问行为数据
    val actionRdd = getActionRDDByDateRange(spark, taskParam)

    printRDD(actionRdd)

    // 将用户行为信息转换成 KV结构
    val sessionid2actionRDD: RDD[(String, UserVisitAction)] = actionRdd.map(action => (action.session_id, action))

    // 数据缓存
    sessionid2actionRDD.cache()

    // 对<sessionid,访问行为> RDD，做一次groupByKey操作，生成页面切片
    val sessionid2actionsRDD: RDD[(String, Iterable[UserVisitAction])] = sessionid2actionRDD.groupByKey()

    // 最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法
    val pageSplitRDD: RDD[(String, Int)] = generateAndMatchPageSplit(spark, sessionid2actionsRDD, taskParam)
    printRDD(pageSplitRDD)

    // 返回：(1_2, 1)，(3_4, 1), ..., (100_101, 1)
    // 统计每个跳转切片的总个数
    // pageSplitPvMap：(1_2, 102320), (3_4, 90021), ..., (100_101, 45789)
    val pageSplitPvMap: collection.Map[String, Long] = pageSplitRDD.countByKey()

    println("-------------------统计每个跳转切片的总个数-----------------------------")
    println(pageSplitPvMap.mkString("|"))
    // 使用者指定的页面流是3,2,5,8,6
    // 咱们现在拿到的这个pageSplitPvMap，3->2，2->5，5->8，8->6

    // 首先计算首页PV的数量
    val startPagePv: Long = getStartPagePv(taskParam, sessionid2actionsRDD)
    println("首先计算首页PV的数量: " + startPagePv)

    // 计算目标页面流的各个页面切片的转化率
    val convertRateMap: mutable.HashMap[String, Double] = computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv)

    // 持久化页面切片转化率
    persistConvertRate(taskUUID, spark, convertRateMap)

    spark.close()
  }


  /**
   * 持久化转化率
   *
   * @param taskUUID
   * @param spark
   * @param convertRateMap
   */
  def persistConvertRate(taskUUID: String, spark: SparkSession, convertRateMap: mutable.HashMap[String, Double]) = {
    val convertRate: String = convertRateMap.map(item => item._1 + "=" + item._2).mkString("|")

    val pageSplitConvertRateRDD: RDD[PageSplitConvertRate] = spark.sparkContext.makeRDD(Array(PageSplitConvertRate(taskUUID, convertRate)))

    import spark.implicits._
    DataUtils.saveRDD2Mysql(pageSplitConvertRateRDD.toDS(), "page_split_convert_rate")
  }

  /**
   * 计算页面切片转化率
   *
   * @param taskParam
   * @param pageSplitPvMap 页面切片pv
   * @param startPagePv    起始页面pv
   * @return
   */
  def computePageSplitConvertRate(taskParam: JSONObject, pageSplitPvMap: collection.Map[String, Long], startPagePv: Long): mutable.HashMap[String, Double] = {
    val convertRateMap = new mutable.HashMap[String, Double]()
    val targetPagePairs: List[String] = getTargetPageFlowPair(taskParam)

    // lastPageSplitPv：存储最新一次的页面PV数量
    var lastPageSplitPv = startPagePv.toDouble

    // 3,5,2,4,6
    // 3_5
    // 3_5 pv / 3 pv
    // 5_2 rate = 5_2 pv / 3_5 pv

    // 通过for循环，获取目标页面流中的各个页面切片（pv）
    for (targetPage <- targetPagePairs) {
      // 先获取pageSplitPvMap中记录的当前targetPage的数量
      val targetPageSplitPv: Double = pageSplitPvMap.get(targetPage).get.toDouble


      // 用当前targetPage的数量除以上一次lastPageSplit的数量，得到转化率
      val convertRate: Double = NumberUtils.formatDouble(targetPageSplitPv / lastPageSplitPv, 2)
      convertRateMap.put(targetPage, convertRate)

      println(s"计算页面转换率: $targetPage : ($targetPageSplitPv, $lastPageSplitPv) -> 转换率:$convertRate")

      // 对targetPage和转化率进行存储
      lastPageSplitPv = targetPageSplitPv
    }

    convertRateMap
  }

  /**
   * 取页面流中初始页面的pv
   *
   * @param taskParam
   * @param sessionid2actionsRDD
   * @return
   */
  def getStartPagePv(taskParam: JSONObject, sessionid2actionsRDD: RDD[(String, Iterable[UserVisitAction])]): Long = {
    // 获取起始页面ID
    val startPageId = getTargetPageFlowList(taskParam)(0).toLong

    // sessionid2actionsRDD是聚合后的用户行为数据
    // userVisitAction中记录的是在一个页面中的用户行为数据
    val startPageRDD: RDD[Long] = sessionid2actionsRDD.flatMap { case (sessionId, userVisitActions) =>
      userVisitActions.filter(_.page_id == startPageId).map(_.page_id)
    }

    // 对PageId等于startPageId的用户行为数据进行技术
    startPageRDD.count()
  }

  /**
   * 获取页面流中初始页面的pv
   *
   * @param spark
   * @param sessionid2actionsRDD
   * @param taskParam
   * @return
   */
  def generateAndMatchPageSplit(spark: SparkSession, sessionid2actionsRDD: RDD[(String, Iterable[UserVisitAction])], taskParam: JSONObject): RDD[(String, Int)] = {
    val targetPagePairs: List[String] = getTargetPageFlowPair(taskParam)

    //将结果转换为广播变量
    //targetPagePairs类型为List[String]
    val targetPageFlowBroadcast: Broadcast[List[String]] = spark.sparkContext.broadcast(targetPagePairs)

    // 对全部数据进行处理
    sessionid2actionsRDD.flatMap { case (sessionid, userVisitActions) =>
      // 获取使用者指定的页面流
      // 使用者指定的页面流，1,2,3,4,5,6,7
      // 1->2的转化率是多少？2->3的转化率是多少？

      // 这里，我们拿到的session的访问行为，默认情况下是乱序的
      // 比如说，正常情况下，我们希望拿到的数据，是按照时间顺序排序的
      // 但是问题是，默认是不排序的
      // 所以，我们第一件事情，对session的访问行为数据按照时间进行排序

      // 举例，反例
      // 比如，3->5->4->10->7
      // 3->4->5->7->10

      // userVisitActions是Iterable[UserAction]，toList.sortWith将Iterable中的所有UserAction按照时间进行排序
      // 按照时间排序
      val sortedUVAs = userVisitActions.toList.sortWith((uva1, uva2) => DateUtils.parseTime(uva1.action_time).getTime() < DateUtils.parseTime(uva2.action_time).getTime())
      // 提取所有UserAction中的PageId信息
      val soredPages = sortedUVAs.map(item => if (item.page_id != null) item.page_id)

      //【注意】页面的PageFlow是将session的所有UserAction按照时间顺序排序后提取PageId,再将PageId进行连接得到的
      // 按照已经排好的顺序对PageId信息进行整合，生成所有页面切片：(1_2,2_3,3_4,4_5,5_6,6_7)
      val sessionPagePairs = soredPages.slice(0, soredPages.length - 1).zip(soredPages.tail).map(item => item._1 + "_" + item._2)

      /* 由此，得到了当前session的PageFlow */

      // 只要是当前session的PageFlow有一个切片与targetPageFlow中任一切片重合，那么就保留下来
      // 目标：(1_2,2_3,3_4,4_5,5_6,6_7)   当前：(1_2,2_5,5_6,6_7,7_8)
      // 最后保留：(1_2,5_6,6_7)
      // 输出：(1_2, 1) (5_6, 1) (6_7, 1)
      sessionPagePairs.filter(targetPageFlowBroadcast.value.contains(_)).map((_, 1))
    }
  }


  /**
   * 获取配置文件中的targetPageFlow
   *
   * @param taskParam
   * @return
   */
  def getTargetPageFlowList(taskParam: JSONObject): List[String] = {
    /* 对目标PageFlow进行解析 */
    //1,2,3,4,5,6,7
    val targetPageFlow: String = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    //将字符串转换成为了List[String]
    val targetPages: List[String] = targetPageFlow.split(",").toList
    targetPages
  }

  /**
   * 将列表转换成切片
   *
   * from : (1,2,3,4,5,6,7)
   * to :   (1_2,2_3,3_4,4_5,5_6,6_7)
   *
   * @param pageList
   * @return
   */
  def calcPagePairs(pageList: List[String]): List[String] = {
    pageList.slice(0, pageList.length - 1).zip(pageList.tail).map(item => item._1 + "_" + item._2)
  }


  /**
   * 获取待转换的页面流切片
   *
   * @param taskParam
   * @return
   */
  def getTargetPageFlowPair(taskParam: JSONObject): List[String] = {
    val targetPages: List[String] = getTargetPageFlowList(taskParam)
    val targetPagePairs: List[String] = calcPagePairs(targetPages)
    targetPagePairs
  }
}
