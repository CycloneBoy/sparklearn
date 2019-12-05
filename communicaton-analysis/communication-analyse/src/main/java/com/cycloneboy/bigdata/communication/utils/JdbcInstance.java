package com.cycloneboy.bigdata.communication.utils;

import java.sql.Connection;
import java.sql.SQLException;

/** Create by sl on 2019-12-05 16:07 */
public class JdbcInstance {

  private static Connection connection = null;

  private JdbcInstance() {}

  public static Connection getConnection() {
    try {
      if (connection == null || connection.isClosed() || connection.isValid(3)) {
        connection = JdbcUtils.getConnection();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return connection;
  }
}
