package com.cycloneboy.bigdata.businessanalysis.commons.pool

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 *
 * Create by  sl on 2019-11-25 12:31
 *
 * MySQL 客户端代理对象
 *
 * @param jdbcUrl      MySQL URL
 * @param jdbcUser     MySQL 用户
 * @param jdbcPassword MySQL 密码
 * @param client       默认客户端实现
 */
case class MySqlProxy(jdbcUrl: String, jdbcUser: String, jdbcPassword: String, client: Option[Connection] = None) {

  // 获取客户端连接对象
  private val mysqlClient = client getOrElse {
    DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
  }

  /**
   * 执行增删改 SQL 语句
   *
   * @param sql
   * @param params
   * @return 影响的行数
   */
  def executeUpdate(sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      // 第一步：关闭自动提交
      mysqlClient.setAutoCommit(false)
      // 第二步：根据传入的 sql 语句创建 prepareStatement
      pstmt = mysqlClient.prepareStatement(sql)

      // 第三步：为 prepareStatement 中的每个参数填写数值
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      // 第四步：执行增删改操作
      rtn = pstmt.executeUpdate()
      // 第五步：手动提交
      mysqlClient.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }
    rtn
  }

  /**
   * 执行查询 SQL 语句
   *
   * @param sql
   * @param params
   */
  def executeQuery(sql: String, params: Array[Any], queryCallback: QueryCallback) {
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null
    try {
      // 第一步：根据传入的 sql 语句创建 prepareStatement
      pstmt = mysqlClient.prepareStatement(sql)
      // 第二步：为 prepareStatement 中的每个参数填写数值
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      // 第三步：执行查询操作
      rs = pstmt.executeQuery()
      // 第四步：处理查询后的结果
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace
    }
  }

  /**
   * 批量执行 SQL 语句
   *
   * @param sql
   * @param paramsList
   * @return 每条 SQL 语句影响的行数
   */
  def executeBatch(sql: String, paramsList: Array[Array[Any]]): Array[Int] = {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    try {
      // 第一步：关闭自动提交
      mysqlClient.setAutoCommit(false)
      pstmt = mysqlClient.prepareStatement(sql)

      // 第二步：为 prepareStatement 中的每个参数填写数值
      if (paramsList != null && paramsList.length > 0) {
        for (params <- paramsList) {
          for (i <- 0 until params.length) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }
      // 第三步：执行批量的 SQL 语句
      rtn = pstmt.executeBatch()
      // 第四步：手动提交
      mysqlClient.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }
    rtn
  }

  // 关闭 MySQL 客户端
  def shutdown(): Unit = mysqlClient.close()
}
