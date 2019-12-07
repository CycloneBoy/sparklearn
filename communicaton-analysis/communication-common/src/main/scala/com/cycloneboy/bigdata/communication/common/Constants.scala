package com.cycloneboy.bigdata.communication.common

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


  val CONF_DURATION_DECIMAL_FORMAT = "duration.decimal.format"
  val CONF_DATE_FORMAT = "date.format"
  val CONF_START_DATE = "start.date"
  val CONF_END_DATE = "end.date"
  val CONF_CALLEE_FLAG = "callee.flag"
  val CONF_LOG_OUTDIR = "log.outDir"
  val CONF_LOG_MOCK_SIZE_PER_SECONDS = "log.mock.size.per.seconds"
  val CONF_LOG_DURAION_MAX_DURATION = "log.duraion.max.duration"
  val CONF_LOG_OUT_DIR = "log.out.dir"

  /**
   * calllog hbase相关配置参数
   */
  val CONF_HBASE_CALLLOG_REGIONCODE_FORMAT = "hbase.calllog.regionCode.format"
  val CONF_HBASE_CALLLOG_NAMESPACE = "hbase.calllog.namespace"
  val CONF_HBASE_CALLLOG_TABLENAME = "hbase.calllog.tablename"
  val CONF_HBASE_CALLLOG_REGIONS = "hbase.calllog.regions"
  val CONF_HBASE_CALLLOG_CALLER_FLAG = "hbase.calllog.caller.flag"
  val CONF_HBASE_CALLLOG_CALLEE_FLAG = "hbase.calllog.callee.flag"
  val CONF_HBASE_CALLLOG_FAMILIES = "hbase.calllog.families"
  val CONF_HBASE_CALLLOG_CALLER_FAMILY = "hbase.calllog.caller.family"
  val CONF_HBASE_CALLLOG_CALLEE_FAMILY = "hbase.calllog.callee.family"

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
   * 表名称
   */
  val TABLE_USER_VISIT_ACTION = "user_visit_action"
  val TABLE_PRODUCT_INFO = "product_info"

  val TABLE_AD_BLACKLIST = "ad_blacklist"
  val TABLE_AD_STAT = "ad_stat"
  val TABLE_AD_USER_CLICK_COUNT = "ad_user_click_count"
  val TABLE_AD_PROVINCE_TOP3 = "ad_province_top3"
  val TABLE_AD_CLICK_TREND = "ad_click_trend"

  /** *********************************************************************************
   * 临时变量
   * 任务相关的常量
   */
  val DATETIME_FORMATE_YYYYMMDD = "yyyy-MM-dd"
  val DATETIME_FORMATE_STANDARD = "yyyy-MM-dd HH:mm:ss"
  val DELIMITER_COMMA = ","
  val DELIMITER_UNDERLINE = "_"

  val DIMENSION_DATE_DEFAULT_MONTH = -1
  val DIMENSION_DATE_DEFAULT_DAY = -1
}
