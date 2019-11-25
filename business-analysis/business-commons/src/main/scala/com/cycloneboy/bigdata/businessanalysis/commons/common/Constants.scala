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


  /** *******************************************************************************
   * 模拟数据生成 数据配置
   */
  val MOCK_DATA_USER_VISIT_ACTION_SEARCH_KEYWORDS = "mock.userVisitAction.searchKeywords"

  /**
   * 用户数量
   */
  val MOCK_NUMBER_OF_USER = 100

  /**
   * 城市数量
   */
  val MOCK_NUMBER_OF_CITY = 100

  /**
   * 产品数量
   */
  val MOCK_NUMBER_OF_PRODUCT = 100

  /**
   * 品类数量
   */
  val MOCK_NUMBER_OF_CATEGORY = 50

  /**
   * 广告数量
   */
  val MOCK_NUMBER_OF_AD = 20


  /** *********************************************************************************
   * hive 表格名称
   */
  val USER_VISIT_ACTION_TABLE = "user_visit_action"
  val USER_INFO_TABLE = "user_info"
  val PRODUCT_INFO_TABLE = "product_info"

}
