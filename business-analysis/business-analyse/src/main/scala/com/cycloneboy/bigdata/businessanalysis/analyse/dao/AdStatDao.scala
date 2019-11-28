package com.cycloneboy.bigdata.businessanalysis.analyse.dao

import java.sql.ResultSet

import com.cycloneboy.bigdata.businessanalysis.analyse.model.AdStat
import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.pool.CreateMySqlPool

import scala.collection.mutable.ArrayBuffer

/**
 *
 * Create by  sl on 2019-11-28 19:15
 */
object AdStatDao {

  def updateBatch(adStats: Array[AdStat]) {
    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()


    // 区分开来哪些是要插入的，哪些是要更新的
    val insertAdStats = ArrayBuffer[AdStat]()
    val updateAdStats = ArrayBuffer[AdStat]()

    val selectSQL = "SELECT count(*) " +
      "FROM  " + Constants.TABLE_AD_STAT + " " +
      "WHERE date=? " +
      "AND province=? " +
      "AND city=? " +
      "AND adid=?"

    for (adStat <- adStats) {

      val params = Array[Any](adStat.date, adStat.province, adStat.city, adStat.adid)
      // 通过查询结果判断当前项时待插入还是待更新
      client.executeQuery(selectSQL, params, (rs: ResultSet) => {
        if (rs.next() && rs.getInt(1) > 0) {
          updateAdStats += adStat
        } else {
          insertAdStats += adStat
        }
      })
    }

    // 对于需要插入的数据，执行批量插入操作
    val insertSQL = "INSERT INTO " + Constants.TABLE_AD_STAT + "  VALUES(?,?,?,?,?)"

    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (adStat <- insertAdStats) {
      insertParamsList += Array[Any](adStat.date, adStat.province, adStat.city, adStat.adid, adStat.clickCount)
    }

    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 对于需要更新的数据，执行批量更新操作
    // 此处的UPDATE是进行覆盖
    val updateSQL = "UPDATE " + Constants.TABLE_AD_STAT + " SET clickCount=? " +
      "WHERE date=? " +
      "AND province=? " +
      "AND city=? " +
      "AND adid=?"

    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (adStat <- updateAdStats) {
      updateParamsList += Array[Any](adStat.clickCount, adStat.date, adStat.province, adStat.city, adStat.adid)
    }

    client.executeBatch(updateSQL, updateParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }
}
