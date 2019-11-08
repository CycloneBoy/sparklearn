package com.cycloneboy.bigdata.flumelearn.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.naming.ConfigurationException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flume.Context;

/**
 * Create by sl on 2019-11-08 21:58 <br>
 * 属性 说明（括号中为默认值） <br>
 * runQueryDelay 查询时间间隔（10000） <br>
 * batchSize 缓存大小（100） <br>
 * startFrom 查询语句开始id（0） <br>
 * currentIndex 查询语句当前id，每次查询之前需要查元数据表 <br>
 * recordSixe 查询返回条数 <br>
 * table 监控的表名 <br>
 * columnsToSelect 查询字段（*） <br>
 * customQuery 用户传入的查询语句 <br>
 * query 查询语句 <br>
 * defaultCharsetResultSet 编码格式（UTF-8）
 */
@Slf4j
@Getter
@Setter
public class SQLSourceHelper {

  private int runQueryDelay, // 两次查询的时间间隔
      startFrom, // 开始id
      currentIndex, // 当前id
      recordSixe = 0, // 每次查询返回结果的条数
      maxRow; // 每次查询的最大条数
  private String table, // 要操作的表
      columnsToSelect, // 用户传入的查询的列
      customQuery, // 用户传入的查询语句
      query, // 构建的查询语句
      defaultCharsetResultSet; // 编码集

  // 上下文，用来获取配置文件
  private Context context;

  // 为定义的变量赋值（默认值），可在flume任务的配置文件中修改
  private static final int DEFAULT_QUERY_DELAY = 10000;
  private static final int DEFAULT_START_VALUE = 0;
  private static final int DEFAULT_MAX_ROWS = 2000;
  private static final String DEFAULT_COLUMNS_SELECT = "*";
  private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";

  private static Connection conn = null;
  private static PreparedStatement ps = null;
  private static String connectionURL, connectionUserName, connectionPassword;

  // 加载静态资源
  static {
    Properties p = new Properties();

    try {
      p.load(SQLSourceHelper.class.getClassLoader().getResourceAsStream("jdbc.properties"));
      connectionURL = p.getProperty("dbUrl");
      connectionUserName = p.getProperty("dbUser");
      connectionPassword = p.getProperty("dbPassword");
      Class.forName(p.getProperty("dbDriver"));
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  /**
   * 获取JDBC连接
   *
   * @param url
   * @param user
   * @param password
   * @return
   */
  private static Connection InitConnection(String url, String user, String password) {

    try {
      Connection conn = DriverManager.getConnection(url, user, password);
      if (conn == null) {
        throw new SQLException();
      }
      return conn;
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  // 构造方法
  public SQLSourceHelper(Context context) throws ParseException, ConfigurationException {
    // 初始化上下文
    this.context = context;

    // 有默认值参数：获取flume任务配置文件中的参数，读不到的采用默认值
    this.columnsToSelect = context.getString("columns.to.select", DEFAULT_COLUMNS_SELECT);
    this.runQueryDelay = context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);
    this.startFrom = context.getInteger("start.from", DEFAULT_START_VALUE);
    this.defaultCharsetResultSet =
        context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);

    // 无默认值参数：获取flume任务配置文件中的参数
    this.table = context.getString("table");
    this.customQuery = context.getString("custom.query");
    connectionURL = context.getString("connection.url");
    connectionUserName = context.getString("connection.user");
    connectionPassword = context.getString("connection.password");
    conn = InitConnection(connectionURL, connectionUserName, connectionPassword);

    // 校验相应的配置信息，如果没有默认值的参数也没赋值，抛出异常
    checkMandatoryProperties();
    // 获取当前的id
    currentIndex = getStatusDBIndex(startFrom);
    // 构建查询语句
    query = buildQuery();
  }

  // 校验相应的配置信息（表，查询语句以及数据库连接的参数）
  private void checkMandatoryProperties() throws ConfigurationException {
    if (table == null) {
      throw new ConfigurationException("property table not set");
    }
    if (connectionURL == null) {
      throw new ConfigurationException("connection.url property not set");
    }
    if (connectionUserName == null) {
      throw new ConfigurationException("connection.user property not set");
    }
    if (connectionPassword == null) {
      throw new ConfigurationException("connection.password property not set");
    }
  }

  // 构建sql语句
  private String buildQuery() {
    String sql = "";
    // 获取当前id
    currentIndex = getStatusDBIndex(startFrom);
    log.info(currentIndex + "");
    if (customQuery == null) {
      sql = "SELECT " + columnsToSelect + " FROM " + table;
    } else {
      sql = customQuery;
    }
    StringBuilder execSql = new StringBuilder(sql);
    // 以id作为offset
    if (!sql.contains("where")) {
      execSql.append(" where ");
      execSql.append("id").append(">").append(currentIndex);
      return execSql.toString();
    } else {
      int length = execSql.toString().length();
      return execSql.toString().substring(0, length - String.valueOf(currentIndex).length())
          + currentIndex;
    }
  }

  // 执行查询
  public List<List<Object>> executeQuery() {
    try {
      // 每次执行查询时都要重新生成sql，因为id不同
      customQuery = buildQuery();
      // 存放结果的集合
      List<List<Object>> results = new ArrayList<>();
      if (ps == null) {
        //
        ps = conn.prepareStatement(customQuery);
      }
      ResultSet result = ps.executeQuery(customQuery);
      while (result.next()) {
        // 存放一条数据的集合（多个列）
        List<Object> row = new ArrayList<>();
        // 将返回结果放入集合
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
          row.add(result.getObject(i));
        }
        results.add(row);
      }
      log.info("execSql:" + customQuery + "\nresultSize:" + results.size());
      return results;
    } catch (SQLException e) {
      log.error(e.toString());
      // 重新连接
      conn = InitConnection(connectionURL, connectionUserName, connectionPassword);
    }
    return null;
  }

  // 将结果集转化为字符串，每一条数据是一个list集合，将每一个小的list集合转化为字符串
  public List<String> getAllRows(List<List<Object>> queryResult) {
    List<String> allRows = new ArrayList<>();
    if (queryResult == null || queryResult.isEmpty()) return allRows;
    StringBuilder row = new StringBuilder();
    for (List<Object> rawRow : queryResult) {
      Object value = null;
      for (Object aRawRow : rawRow) {
        value = aRawRow;
        if (value == null) {
          row.append(",");
        } else {
          row.append(aRawRow.toString()).append(",");
        }
      }
      allRows.add(row.toString());
      row = new StringBuilder();
    }
    return allRows;
  }

  // 更新offset元数据状态，每次返回结果集后调用。必须记录每次查询的offset值，为程序中断续跑数据时使用，以id为offset
  public void updateOffset2DB(int size) {
    // 以source_tab做为KEY，如果不存在则插入，存在则更新（每个源表对应一条记录）
    String sql =
        "insert into flume_meta(source_tab,currentIndex) VALUES('"
            + this.table
            + "','"
            + (recordSixe += size)
            + "') on DUPLICATE key update source_tab=values(source_tab),currentIndex=values(currentIndex)";
    log.info("updateStatus Sql:" + sql);
    execSql(sql);
  }

  // 执行sql语句
  private void execSql(String sql) {
    try {
      ps = conn.prepareStatement(sql);
      log.info("exec::" + sql);
      ps.execute();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  // 获取当前id的offset
  private Integer getStatusDBIndex(int startFrom) {
    // 从flume_meta表中查询出当前的id是多少
    String dbIndex =
        queryOne("select currentIndex from flume_meta where source_tab='" + table + "'");
    if (dbIndex != null) {
      return Integer.parseInt(dbIndex);
    }
    // 如果没有数据，则说明是第一次查询或者数据表中还没有存入数据，返回最初传入的值
    return startFrom;
  }

  // 查询一条数据的执行语句(当前id)
  private String queryOne(String sql) {
    ResultSet result = null;
    try {
      ps = conn.prepareStatement(sql);
      result = ps.executeQuery();
      while (result.next()) {
        return result.getString(1);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  // 关闭相关资源
  public void close() {
    try {
      ps.close();
      conn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
