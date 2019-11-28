package com.cycloneboy.bigdata.businessanalysis.analyse.dao

import java.sql.ResultSet

import com.cycloneboy.bigdata.businessanalysis.analyse.model.AdUserClickCount
import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.pool.CreateMySqlPool

import scala.collection.mutable.ArrayBuffer

/**
 *
 * 广告实时统计DAO实现类
 *
 * Create by  sl on 2019-11-28 14:31
 *
 */
object AdUserClickCountDao {

  def updateBatch(adUserClickCounts: Array[AdUserClickCount]) = {
    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    // 区分开来哪些是要插入的，哪些是要更新的
    val insertAdUserClickCounts = ArrayBuffer[AdUserClickCount]()
    val updateAdUserClickCounts = ArrayBuffer[AdUserClickCount]()

    val selectSql = "select count(*) from " + Constants.TABLE_AD_USER_CLICK_COUNT +
      " where date=? and userid=?  and adid=?"

    for (adUserClickCount <- adUserClickCounts) {
      val params = Array[Any](adUserClickCount.date, adUserClickCount.userid, adUserClickCount.adid)
      // 通过查询结果判断当前项时待插入还是待更新
      client.executeQuery(selectSql, params, (result: ResultSet) => {
        if (result.next() && result.getInt(1) > 0) {
          updateAdUserClickCounts += adUserClickCount
        } else {
          insertAdUserClickCounts += adUserClickCount
        }
      })
    }

    // 对于需要插入的数据，执行批量插入操作
    val insertSql: String = "insert into " + Constants.TABLE_AD_USER_CLICK_COUNT + " values(?,?,?,?)"
    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (adUserClickCount <- adUserClickCounts) {
      insertParamsList += Array[Any](adUserClickCount.date, adUserClickCount.userid, adUserClickCount.adid, adUserClickCount.clickCount)
    }

    client.executeBatch(insertSql, insertParamsList.toArray)

    // 对于需要更新的数据，执行批量更新操作
    // 此处的UPDATE是进行覆盖
    val updateSql: String = "update  " + Constants.TABLE_AD_USER_CLICK_COUNT + " set clickCount=? " +
      " where date=? and userid=? and adid=?"
    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (adUserClickCount <- adUserClickCounts) {
      updateParamsList += Array[Any](adUserClickCount.clickCount, adUserClickCount.date, adUserClickCount.userid, adUserClickCount.adid)
    }

    client.executeBatch(updateSql, updateParamsList.toArray)
    mySqlPool.returnObject(client)
  }

  /**
   * 根据多个key查询用户广告点击量
   *
   * @param date
   * @param userid
   * @param adid
   */
  def findClickCountByMutiKey(date: String, userid: Long, adid: Long) = {
    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    val sql = "select clickCount from " + Constants.TABLE_AD_USER_CLICK_COUNT +
      " where date=? and userid=? and adid=?"

    var clickcount = 0
    val params = Array[Any](date, userid, adid)

    client.executeQuery(sql, params, (result: ResultSet) => {
      if (result.next()) {
        clickcount = result.getInt(1)
      }
    })

    mySqlPool.returnObject(client)

    clickcount
  }
}



