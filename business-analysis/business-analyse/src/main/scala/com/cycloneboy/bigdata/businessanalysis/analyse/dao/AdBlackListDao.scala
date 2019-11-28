package com.cycloneboy.bigdata.businessanalysis.analyse.dao

import java.sql.ResultSet

import com.cycloneboy.bigdata.businessanalysis.analyse.model.AdBlacklist
import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.pool.CreateMySqlPool

import scala.collection.mutable.ArrayBuffer

/**
 *
 * Create by  sl on 2019-11-28 14:19
 */
object AdBlackListDao {

  /**
   * 批量插入广告黑名单用户
   *
   * @param adBlackLists
   */
  def insertBatch(adBlackLists: Array[AdBlacklist]) = {
    val sql = "insert into " + Constants.TABLE_AD_BLACKLIST + " value(?)"

    val paramsList = new ArrayBuffer[Array[Any]]()

    for (adBlacklist <- adBlackLists) {
      val params = Array[Any](adBlacklist.userid)
      paramsList += params
    }

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    client.executeBatch(sql, paramsList.toArray)

    //    println("--------------------AdBlackListDao.insertBatch------------------------")
    //    adBlackLists.foreach(println(_))
    //    println("----------------------------------------------------------------------")
    mySqlPool.returnObject(client)
  }

  /**
   * 查询所有广告黑名单用户
   *
   */
  def findAll(): Array[AdBlacklist] = {
    val sql = "select * from " + Constants.TABLE_AD_BLACKLIST
    val adBlacklists = new ArrayBuffer[AdBlacklist]()

    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    client.executeQuery(sql, null, (result: ResultSet) => {
      while (result.next()) {
        val userid = result.getInt(1).toLong
        adBlacklists += AdBlacklist(userid)
      }
    })
    mySqlPool.returnObject(client)
    adBlacklists.toArray
  }


}
