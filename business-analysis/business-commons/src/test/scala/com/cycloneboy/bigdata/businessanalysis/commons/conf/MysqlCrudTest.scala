package com.cycloneboy.bigdata.businessanalysis.commons.conf

import java.sql.ResultSet

import com.cycloneboy.bigdata.businessanalysis.commons.model.AdBlackList
import com.cycloneboy.bigdata.businessanalysis.commons.pool.{CreateMySqlPool, QueryCallback}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
 *
 * Create by  sl on 2019-11-25 14:24
 */
class MysqlCrudTest {

  @Test def testSelect = {

    val mysqlPool = CreateMySqlPool()
    val client = mysqlPool.borrowObject()

    val sql = "select * from ad_blacklist"
    val adBlackLists = new ArrayBuffer[AdBlackList]()
    client.executeQuery(sql, null, new QueryCallback {
      override def process(result: ResultSet): Unit = {
        while (result.next()) {
          val userId = result.getInt(1).toLong
          adBlackLists += AdBlackList(userId)
        }
      }
    })

    mysqlPool.returnObject(client)
    for (elem <- adBlackLists.toArray) {
      println(elem)
    }
  }
}
