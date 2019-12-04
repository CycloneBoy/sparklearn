package com.cycloneboy.bigdata.communication.utils;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/** Create by sl on 2019-12-04 12:51 */
public class HBaseConnectionInstance {

  private static Connection conn;

  public static synchronized Connection getConnection(Configuration conf) {
    try {
      if (conn == null || conn.isClosed()) {

        conn = ConnectionFactory.createConnection(conf);
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    return conn;
  }
}
