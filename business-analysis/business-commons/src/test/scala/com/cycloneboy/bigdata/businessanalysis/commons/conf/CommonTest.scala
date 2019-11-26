package com.cycloneboy.bigdata.businessanalysis.commons.conf

import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.utils.{DateUtils, ValidUtils}
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

  @Test def testValidUtilIn() = {
    val source = "火锅,蛋糕,烧烤"
    val target1 = "火锅,串串香,iphone手机"
    val target2 = "火锅1,串串香1,iphone手机1"
    val target3 = "火锅,串串香1,iphone手机1"
    val target4 = ""

    println(ValidUtils.in(source, target1))
    println(ValidUtils.in(source, target2))
    println(ValidUtils.in(source, target3))
    println(ValidUtils.in(source, target4))

    "123".split(",").toSet foreach println
    "123,456".split(",").toSet foreach println
  }

  @Test def testFormatDateTime() = {
    println(DateUtils.getTodayStandard())
  }
}
