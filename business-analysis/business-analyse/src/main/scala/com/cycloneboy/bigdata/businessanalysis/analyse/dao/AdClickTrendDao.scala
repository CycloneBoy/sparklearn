package com.cycloneboy.bigdata.businessanalysis.analyse.dao

import java.sql.ResultSet

import com.cycloneboy.bigdata.businessanalysis.analyse.model.AdClickTrend
import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.pool.CreateMySqlPool

import scala.collection.mutable.ArrayBuffer

/**
 * 广告点击趋势DAO实现类
 *
 * Create by  sl on 2019-11-28 15:01
 */
object AdClickTrendDao {

  def updateBatch(adClickTrends: Array[AdClickTrend]) = {
    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    // 区分开来哪些是要插入的，哪些是要更新的
    val insertAdClickTrends = ArrayBuffer[AdClickTrend]()
    val updateAdClickTrends = ArrayBuffer[AdClickTrend]()

    val selectSql = "select count(*) from " + Constants.TABLE_AD_CLICK_TREND +
      " where date=? and hour=? and minute=? and adid=?"

    for (adClickTrend <- adClickTrends) {
      val params = Array[Any](adClickTrend.date, adClickTrend.hour, adClickTrend.minute, adClickTrend.adid)
      // 通过查询结果判断当前项时待插入还是待更新
      client.executeQuery(selectSql, params, (result: ResultSet) => {
        if (result.next() && result.getInt(1) > 0) {
          updateAdClickTrends += adClickTrend
        } else {
          insertAdClickTrends += adClickTrend
        }
      })
    }

    // 对于需要插入的数据，执行批量插入操作
    val insertSql: String = "insert into " + Constants.TABLE_AD_CLICK_TREND + " values(?,?,?,?,?)"
    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (adClickTrend <- adClickTrends) {
      insertParamsList += Array[Any](adClickTrend.date, adClickTrend.hour, adClickTrend.minute, adClickTrend.adid, adClickTrend.clickCount)
    }

    client.executeBatch(insertSql, insertParamsList.toArray)

    // 对于需要更新的数据，执行批量更新操作
    // 此处的UPDATE是进行覆盖
    val updateSql: String = "update  " + Constants.TABLE_AD_CLICK_TREND + " set clickCount=? " +
      " where date=? and hour=? and minute=? and adid=?"
    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (adClickTrend <- adClickTrends) {
      updateParamsList += Array[Any](adClickTrend.clickCount, adClickTrend.date, adClickTrend.hour, adClickTrend.minute, adClickTrend.adid)
    }

    client.executeBatch(updateSql, updateParamsList.toArray)
    mySqlPool.returnObject(client)
  }
}
