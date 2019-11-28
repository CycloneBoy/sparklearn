package com.cycloneboy.bigdata.businessanalysis.commons.common

/**
 *
 * Create by  sl on 2019-11-25 14:00
 */
object Constants {

  /** *********************************************************************************
   * 配置参数
   */
  val CONF_JDBC_URL = "jdbc.url"
  val CONF_JDBC_DRIVER = "jdbc.driver"
  val CONF_JDBC_USER = "jdbc.user"
  val CONF_JDBC_PASSWORD = "jdbc.password"
  val CONF_JDBC_DATASOURCE_SIZE = "jdbc.datasource.size"


  val CONF_KAFKA_BROKER_LIST = "kafka.broker.list"
  val CONF_KAFKA_TOPICS = "kafka.topics"
  val CONF_AD_BLACK_LIST_FILTER_MAX = "ad.blacklist.filter.max"


  /** *******************************************************************************
   * 模拟数据生成 数据配置
   */
  val MOCK_DATA_USER_VISIT_ACTION_SEARCH_KEYWORDS = "mock.userVisitAction.searchKeywords"

  /**
   * 用户数量
   */
  val MOCK_NUMBER_OF_USER = "mock.number.of.user"

  /**
   * 城市数量
   */
  val MOCK_NUMBER_OF_CITY = "mock.number.of.city"

  /**
   * 产品数量
   */
  val MOCK_NUMBER_OF_PRODUCT = "mock.number.of.product"

  /**
   * 品类数量
   */
  val MOCK_NUMBER_OF_CATEGORY = "mock.number.of.category"

  /**
   * 广告数量
   */
  val MOCK_NUMBER_OF_AD = "mock.number.of.ad"


  /** *********************************************************************************
   * hive 表格名称
   */
  val USER_VISIT_ACTION_TABLE = "user_visit_action"
  val USER_INFO_TABLE = "user_info"
  val PRODUCT_INFO_TABLE = "product_info"


  /** *********************************************************************************
   * 分析任务入口参数 TASK_PARAMS
   * 任务相关的常量
   */

  val TASK_PARAMS = "task.params.json"

  val PARAM_START_DATE = "startDate"
  val PARAM_END_DATE = "endDate"
  val PARAM_START_AGE = "startAge"
  val PARAM_END_AGE = "endAge"
  val PARAM_PROFESSIONALS = "professionals"
  val PARAM_CITIES = "cities"
  val PARAM_SEX = "sex"
  val PARAM_KEYWORDS = "keywords"
  val PARAM_CATEGORY_IDS = "categoryIds"
  val PARAM_TARGET_PAGE_FLOW = "targetPageFlow"

  /**
   * 任务二: 需要随机抽取session的数量
   */
  val PARAM_EXTRACT_SESSION_NUMBER = "extractSessionNumber"

  /**
   * *********************************************************************************
   * 临时变量
   */
  val SESSION_COUNT = "session_count"

  /**
   * 访问时间步长
   */
  val TIME_PERIOD_1s_3s = "1s_3s"
  val TIME_PERIOD_4s_6s = "4s_6s"
  val TIME_PERIOD_7s_9s = "7s_9s"
  val TIME_PERIOD_10s_30s = "10s_30s"
  val TIME_PERIOD_30s_60s = "30s_60s"
  val TIME_PERIOD_1m_3m = "1m_3m"
  val TIME_PERIOD_3m_10m = "3m_10m"
  val TIME_PERIOD_10m_30m = "10m_30m"
  val TIME_PERIOD_30m = "30m"

  /**
   * 访问页面步长
   */
  val STEP_PERIOD_1_3 = "1_3"
  val STEP_PERIOD_4_6 = "4_6"
  val STEP_PERIOD_7_9 = "7_9"
  val STEP_PERIOD_10_30 = "10_30"
  val STEP_PERIOD_30_60 = "30_60"
  val STEP_PERIOD_60 = "60"

  /**
   * 表名称
   */
  val TABLE_USER_VISIT_ACTION = "user_visit_action"
  val TABLE_PRODUCT_INFO = "product_info"

  val TABLE_AD_BLACKLIST = "ad_blacklist"
  val TABLE_AD_STAT = "ad_stat"
  val TABLE_AD_USER_CLICK_COUNT = "ad_user_click_count"
  val TABLE_AD_PROVINCE_TOP3 = "ad_province_top3"
  val TABLE_AD_CLICK_TREND = "ad_click_trend"
}
