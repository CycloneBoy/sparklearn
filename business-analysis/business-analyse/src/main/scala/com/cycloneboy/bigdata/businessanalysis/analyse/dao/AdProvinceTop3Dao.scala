package com.cycloneboy.bigdata.businessanalysis.analyse.dao

import com.cycloneboy.bigdata.businessanalysis.analyse.model.AdProvinceTop3
import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.pool.CreateMySqlPool

import scala.collection.mutable.ArrayBuffer

/**
 *
 * Create by  sl on 2019-11-28 14:46
 */
object AdProvinceTop3Dao {

  /**
   * 批量更新,
   * 先删除之前的数据,然后更新或者删除现有的数据
   *
   * @param adProvinceTop3s
   */
  def updateBatch(adProvinceTop3s: Array[AdProvinceTop3]) = {
    val mySqlPool = CreateMySqlPool()
    val client = mySqlPool.borrowObject()

    // dateProvinces可以实现一次去重
    // AdProvinceTop3：date province adid clickCount，由于每条数据由date province adid组成
    // 当只取date province时，一定会有重复的情况
    val dateProvinces = ArrayBuffer[String]()

    for (adProvinceTop3 <- adProvinceTop3s) {
      val key = adProvinceTop3.date + "_" + adProvinceTop3.province

      // dateProvinces中不包含当前key才添加
      // 借此去重
      if (!dateProvinces.contains(key)) {
        dateProvinces += key
      }
    }

    // 根据去重后的date和province，进行批量删除操作
    // 先将原来的数据全部删除
    val deleteSql = "delete from " + Constants.TABLE_AD_PROVINCE_TOP3 + " where date=? and province=? "
    val deleteParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (dateProvince <- dateProvinces) {
      val dateProvinceSplicted = dateProvince.split("_")
      val date = dateProvinceSplicted(0)
      val province = dateProvinceSplicted(1)

      val params = Array[Any](date, province)
      deleteParamsList += params
    }

    client.executeBatch(deleteSql, deleteParamsList.toArray)

    // 批量插入传入进来的所有数据
    val insertSql = "insert into " + Constants.TABLE_AD_PROVINCE_TOP3 + " value(?,?,?,?)"

    val paramsList = new ArrayBuffer[Array[Any]]()

    for (adProvinceTop3 <- adProvinceTop3s) {
      val params = Array[Any](adProvinceTop3.date, adProvinceTop3.province, adProvinceTop3.adid, adProvinceTop3.clickCount)
      paramsList += params
    }

    client.executeBatch(insertSql, paramsList.toArray)

    mySqlPool.returnObject(client)
  }
}