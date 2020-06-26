package com.cycloneboy.bigdata.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Create by  sl on 2020-06-26 23:04
 */
public class TestPhoenixJDBC {


  private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";

  public static void main(String[] args) throws SQLException {

    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    conn = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
    stmt = conn.createStatement();
    stmt.execute("drop table if exists test");
    stmt.execute("create table test (mykey integer not null primary key, mycolumn varchar)");
//    stmt.execute("create index test_idx on test(mycolumn)");
    stmt.executeUpdate("upsert into test values (1,'World!')");
    stmt.executeUpdate("upsert into test values (2,'Hello')");
    stmt.executeUpdate("upsert into test values (3,'World!')");
    conn.commit();
    rs = stmt.executeQuery("select mykey from test where mycolumn='Hello'");

    while (rs.next()) {
      System.out.println(rs.getInt(1));
    }

    stmt.close();
    rs.close();
    conn.close();

  }

}
