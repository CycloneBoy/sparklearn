package com.cycloneboy.bigdata.businessanalysis.commons.conf

import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import org.junit.Assert._
import org.junit.Test

/**
 *
 * Create by  sl on 2019-11-25 14:05
 */
class CommonTest {


  @Test def testGetConfig() = {
    val jdbcUrl = ConfigurationManager.config.getString(Constants.CONF_JDBC_URL)
    println(jdbcUrl)
    assertEquals(jdbcUrl, "jdbc:mysql://localhost:3306/business?useUnicode=true&characterEncoding=utf8")
  }

  @Test def testGetArray() = {
    val searchKeywords = ConfigurationManager.config.getString(Constants.MOCK_DATA_USER_VISIT_ACTION_SEARCH_KEYWORDS).split(",")

    searchKeywords foreach println
  }
}
