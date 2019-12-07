package com.cycloneboy.bigdata.communication.converter;

import com.cycloneboy.bigdata.communication.kv.base.BaseDimension;
import com.cycloneboy.bigdata.communication.kv.key.ContactDimension;
import com.cycloneboy.bigdata.communication.kv.key.DateDimension;
import com.cycloneboy.bigdata.communication.utils.JdbcInstance;
import com.cycloneboy.bigdata.communication.utils.JdbcUtils;
import com.cycloneboy.bigdata.communication.utils.LRUCache;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/**
 * Create by sl on 2019-12-05 15:46
 *
 * <p>1、根据传入的维度数据，得到该数据对应的在表中的主键id
 *
 * <p>做内存缓存，LRUCache
 *
 * <p>分支
 *
 * <p>-- 缓存中有数据 -> 直接返回id
 *
 * <p>-- 缓存中无数据 ->
 *
 * <p>查询Mysql
 *
 * <p>分支：
 *
 * <p>-- Mysql中有该条数据 -> 直接返回id -> 将本次读取到的id缓存到内存中
 *
 * <p>-- Mysql中没有该数据 -> 插入该条数据 -> 再次反查该数据，得到id并返回 -> 缓存到内存中
 */
@Slf4j
public class DimensionConverterImpl implements DimensionConverter {

  // 对象线程化 用于每个线程管理自己的JDBC连接器
  private ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();

  // 构建内存缓存对象
  private LRUCache lruCache = new LRUCache(3000);

  public DimensionConverterImpl() {
    // jvm关闭时，释放资源
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(() -> JdbcUtils.close(connectionThreadLocal.get(), null, null)));
  }

  @Override
  public int getDimensionId(BaseDimension dimension) {
    // 1、根据传入的维度对象获取对应的主键id，先从LRUCache中获取
    // 时间维度：date_dimension_year_month_day, 10
    // 联系人维度：contact_dimension_telephone, 12
    String cacheKey = genCacheKey(dimension);

    if (lruCache.containsKey(cacheKey)) {
      return lruCache.get(cacheKey);
    }

    // 没有得到缓存id，需要执行select操作
    // sqls包含了1组sql语句：查询和插入
    String[] sqls = null;

    if (dimension instanceof DateDimension) {
      sqls = getDateDimensionSql();
    } else if (dimension instanceof ContactDimension) {
      sqls = getContactDimensionSql();
    } else {
      throw new RuntimeException("没有匹配到对应的统计维度:DateDimension,ContactDimension");
    }

    // 准备对Mysql表进行操作，先查询，有可能再插入
    Connection connection = this.getConnection();

    int id = -1;
    synchronized (this) {
      id = execSql(connection, sqls, dimension);
    }

    // 将刚查询到的id加入到缓存中
    lruCache.put(cacheKey, id);
    return id;
  }

  /**
   * 得到当前线程维护的Connection对象
   *
   * @return Connection
   */
  private Connection getConnection() {
    Connection connection = null;
    connection = connectionThreadLocal.get();

    try {
      if (connection == null || connection.isClosed()) {
        connection = JdbcInstance.getConnection();
        connectionThreadLocal.set(connection);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return connection;
  }

  /**
   * 执行SQL <br>
   * 1. 先查询ID<br>
   * 2. 查询不到则插入数据<br>
   * 3. 然后查询新插入的数据的ID<br>
   * 4. 返回ID<br>
   *
   * @param connection JDBC连接器
   * @param sqls 长度为2，第一个位置为查询语句，第二个位置为插入语句
   * @param dimension 统计维度:DateDimension,ContactDimension
   * @return 查询到的数据ID
   */
  private int execSql(Connection connection, String[] sqls, BaseDimension dimension) {
    PreparedStatement preparedStatement = null;
    ResultSet resultset = null;

    try {
      // 1
      // 查询的preparedStatement
      preparedStatement = connection.prepareStatement(sqls[0]);
      //      log.info("preparedStatement: {}", preparedStatement.toString());

      // 根据不同的维度，封装不同的SQL语句
      setArguments(preparedStatement, dimension);
      resultset = preparedStatement.executeQuery();
      if (resultset.next()) {
        int resultId = resultset.getInt(1);
        // 释放资源
        JdbcUtils.close(connection, preparedStatement, resultset);
        return resultId;
      }

      // 2
      // 执行插入，封装插入的sql语句
      preparedStatement = connection.prepareStatement(sqls[1]);

      // 根据不同的维度，封装不同的SQL语句
      setArguments(preparedStatement, dimension);
      preparedStatement.executeUpdate();

      // 3
      // 查询的preparedStatement
      preparedStatement = connection.prepareStatement(sqls[0]);

      // 根据不同的维度，封装不同的SQL语句
      setArguments(preparedStatement, dimension);
      resultset = preparedStatement.executeQuery();
      if (resultset.next()) {
        return resultset.getInt(1);
      }

    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      // 释放资源
      JdbcUtils.close(connection, preparedStatement, resultset);
    }

    return -1;
  }

  /**
   * 设置SQL语句的具体参数
   *
   * @param preparedStatement PreparedStatement
   * @param dimension 统计维度:DateDimension,ContactDimension
   */
  private PreparedStatement setArguments(
      PreparedStatement preparedStatement, BaseDimension dimension) {

    int index = 0;
    try {

      if (dimension instanceof DateDimension) {

        // 可以优化
        DateDimension dateDimension = (DateDimension) dimension;
        preparedStatement.setInt(++index, dateDimension.getYear());
        preparedStatement.setInt(++index, dateDimension.getMonth());
        preparedStatement.setInt(++index, dateDimension.getDay());

      } else if (dimension instanceof ContactDimension) {
        ContactDimension contactDimension = (ContactDimension) dimension;
        preparedStatement.setString(++index, contactDimension.getTelephone());
        preparedStatement.setString(++index, contactDimension.getName());
      } else {
        throw new RuntimeException("没有匹配到对应的统计维度:DateDimension,ContactDimension");
      }

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return preparedStatement;
  }

  /**
   * 返回联系人表中的查询和插入SQL
   *
   * @return SQL
   */
  private String[] getContactDimensionSql() {
    String query = "select id from tb_contacts where telephone = ? and name = ? order by id;";
    String insert = "insert into tb_contacts (telephone, name) values (?,?);";
    return new String[] {query, insert};
  }

  /**
   * 返回时间表的查询和插入语句
   *
   * @return SQL
   */
  private String[] getDateDimensionSql() {
    String query =
        "select id from tb_dimension_date where year = ? and month = ? and day = ? order by id;";
    String insert = "insert into tb_dimension_date (year,month, day) values (?,?,?);";
    return new String[] {query, insert};
  }

  /**
   * 根据维度信息得到维度对应的缓存键
   *
   * @param dimension
   * @return
   */
  private String genCacheKey(BaseDimension dimension) {
    StringBuilder sb = new StringBuilder();
    if (dimension instanceof DateDimension) {
      DateDimension dateDimension = (DateDimension) dimension;
      sb.append("date_dimension")
          .append(dateDimension.getYear())
          .append(dateDimension.getMonth())
          .append(dateDimension.getDay());

    } else if (dimension instanceof ContactDimension) {
      ContactDimension contactDimension = (ContactDimension) dimension;
      sb.append("contact_dimension").append(contactDimension.getTelephone());
    }

    return sb.toString();
  }
}
