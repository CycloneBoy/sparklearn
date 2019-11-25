package com.cycloneboy.bigdata.businessanalysis.commons.conf

import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import org.junit.Assert._
import org.junit.Test

/**
 *
 * Create by  sl on 2019-11-25 14:05
 */
class CommonTest {


  @Test def TestGetConfig() = {
    val jdbcUrl = ConfigurationManager.config.getString(Constants.CONF_JDBC_URL)
    println(jdbcUrl)
    assertEquals(jdbcUrl, "jdbc:mysql://localhost:3306/business?useUnicode=true&characterEncoding=utf8")
  }
}
