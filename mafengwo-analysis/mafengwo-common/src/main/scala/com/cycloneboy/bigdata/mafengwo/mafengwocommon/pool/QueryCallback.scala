package com.cycloneboy.bigdata.mafengwo.mafengwocommon.pool

import java.sql.ResultSet

/**
 *
 * Create by  sl on 2019-11-25 12:35
 *
 * 执行sql查询之后的回调函数
 */
trait QueryCallback {

  def process(result: ResultSet)

}
