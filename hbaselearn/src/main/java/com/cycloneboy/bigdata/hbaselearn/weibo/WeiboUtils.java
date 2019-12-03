package com.cycloneboy.bigdata.hbaselearn.weibo;

import static com.cycloneboy.bigdata.hbaselearn.common.Constants.COLUMN_BLOCK_SIZE;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.COLUMN_FAMILY_ATTENDS;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.COLUMN_FAMILY_FANS;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.COLUMN_FAMILY_INFO;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.DELIMITER_UNDERLINE;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.NAMESPACE_WEIBO;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.QUALIFIER_CONTENT;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.TABLE_CONTENT;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.TABLE_INBOX;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.TABLE_RELATION;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.WEIBO_INBOX_GET_MAX_VERSION;
import static com.cycloneboy.bigdata.hbaselearn.common.Constants.WEIBO_INBOX_MAX_VERSION;

import com.cycloneboy.bigdata.hbaselearn.utils.HbaseUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/** Create by sl on 2019-12-02 23:43 */
@Slf4j
public class WeiboUtils {

  private static Configuration conf = HBaseConfiguration.create();

  static {
    // 使用HBaseConfiguration的单例方法实例化
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
  }

  /** 初始化方法 */
  public void initTable() {
    initNamespace();
    createTableContent();
    createTableRelation();
    createTableInbox();
  }

  /** 初始化命名空间 */
  public void initNamespace() {
    HBaseAdmin admin = null;
    try {
      Connection connection = ConnectionFactory.createConnection(conf);

      admin = (HBaseAdmin) connection.getAdmin();

      //      // 判断是否存在此namespace
      //      boolean existNameSpaceflag = false;
      //      try {
      //        admin.getNamespaceDescriptor(NAMESPACE_WEIBO);
      //      } catch (IOException e) {
      //        e.printStackTrace();
      //        existNameSpaceflag = true;
      //      }
      //      if (existNameSpaceflag) {
      //        return;
      //      }

      NamespaceDescriptor weibo =
          NamespaceDescriptor.create(NAMESPACE_WEIBO)
              .addConfiguration("creator", "sl")
              .addConfiguration("create_time", System.currentTimeMillis() + "")
              .build();

      admin.createNamespace(weibo);
      log.info("初始化命名空间");
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (admin != null) {
        try {
          admin.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * 创建微博内容表 <br>
   * Table Name : weibo:content <br>
   * RowKey: 用户ID_时间戳 <br>
   * ColumnFamily:info <br>
   * ColumnLabel: 标题,内容,图片URL <br>
   * Version:1个版本
   */
  public void createTableContent() {
    HBaseAdmin admin = null;
    try {
      Connection connection = ConnectionFactory.createConnection(conf);

      admin = (HBaseAdmin) connection.getAdmin();

      if (HbaseUtils.isTableExist(TABLE_CONTENT)) {
        log.info("表 {} 已经存在 ", TABLE_CONTENT);
        return;
      }

      // 创建表表述
      HTableDescriptor contentTableDescriptor =
          new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));
      // 创建列族描述
      HColumnDescriptor infoColumnDescriptor =
          new HColumnDescriptor(Bytes.toBytes(COLUMN_FAMILY_INFO));

      // 设置块缓存
      infoColumnDescriptor.setBlockCacheEnabled(true);
      // 设置块缓存大小
      infoColumnDescriptor.setBlocksize(COLUMN_BLOCK_SIZE);
      // 设置压缩方式
      //      infoColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
      // 设置版本确界
      infoColumnDescriptor.setMaxVersions(1);
      infoColumnDescriptor.setMinVersions(1);

      contentTableDescriptor.addFamily(infoColumnDescriptor);
      admin.createTable(contentTableDescriptor);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (admin != null) {
        try {
          admin.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * 创建用户关系表 <br>
   * Table Name : weibo:relation <br>
   * RowKey: 用户ID <br>
   * ColumnFamily:attends,fans <br>
   * ColumnLabel: 关注用户ID,粉丝用户ID <br>
   * ColumnValue:用户ID <br>
   * Version:1个版本
   */
  public void createTableRelation() {
    HBaseAdmin admin = null;
    try {
      Connection connection = ConnectionFactory.createConnection(conf);

      admin = (HBaseAdmin) connection.getAdmin();
      if (HbaseUtils.isTableExist(TABLE_RELATION)) {
        log.info("表 {} 已经存在 ", TABLE_RELATION);
        return;
      }
      // 创建表表述
      HTableDescriptor relationTableDescriptor =
          new HTableDescriptor(TableName.valueOf(TABLE_RELATION));
      // 创建列族描述
      HColumnDescriptor attendsColumnDescriptor =
          new HColumnDescriptor(Bytes.toBytes(COLUMN_FAMILY_ATTENDS));

      // 设置块缓存
      attendsColumnDescriptor.setBlockCacheEnabled(true);
      // 设置块缓存大小
      attendsColumnDescriptor.setBlocksize(COLUMN_BLOCK_SIZE);
      // 设置压缩方式
      //      infoColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
      // 设置版本确界
      attendsColumnDescriptor.setMaxVersions(1);
      attendsColumnDescriptor.setMinVersions(1);

      // 创建列族描述
      HColumnDescriptor fansColumnDescriptor =
          new HColumnDescriptor(Bytes.toBytes(COLUMN_FAMILY_FANS));

      // 设置块缓存
      fansColumnDescriptor.setBlockCacheEnabled(true);
      // 设置块缓存大小
      fansColumnDescriptor.setBlocksize(COLUMN_BLOCK_SIZE);
      // 设置压缩方式
      //      fansColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
      // 设置版本确界
      fansColumnDescriptor.setMaxVersions(1);
      fansColumnDescriptor.setMinVersions(1);

      relationTableDescriptor.addFamily(attendsColumnDescriptor);
      relationTableDescriptor.addFamily(fansColumnDescriptor);
      admin.createTable(relationTableDescriptor);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (admin != null) {
        try {
          admin.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * 创建微博收件箱表 <br>
   * Table Name : weibo:inbox <br>
   * RowKey: 用户ID <br>
   * ColumnFamily:info <br>
   * ColumnLabel: 用户ID_发布微博的人的用户ID <br>
   * ColumnValue:关注的人的微博的Rowkey Version:1000个版本
   */
  public void createTableInbox() {
    HBaseAdmin admin = null;
    try {
      Connection connection = ConnectionFactory.createConnection(conf);

      if (HbaseUtils.isTableExist(TABLE_INBOX)) {
        log.info("表 {} 已经存在 ", TABLE_INBOX);
        return;
      }

      admin = (HBaseAdmin) connection.getAdmin();

      // 创建表表述
      HTableDescriptor inboxTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_INBOX));
      // 创建列族描述
      HColumnDescriptor infoColumnDescriptor =
          new HColumnDescriptor(Bytes.toBytes(COLUMN_FAMILY_INFO));

      // 设置块缓存
      infoColumnDescriptor.setBlockCacheEnabled(true);
      // 设置块缓存大小
      infoColumnDescriptor.setBlocksize(COLUMN_BLOCK_SIZE);
      // 设置压缩方式
      //      infoColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
      // 设置版本确界
      infoColumnDescriptor.setMaxVersions(WEIBO_INBOX_MAX_VERSION);
      infoColumnDescriptor.setMinVersions(WEIBO_INBOX_MAX_VERSION);

      inboxTableDescriptor.addFamily(infoColumnDescriptor);
      admin.createTable(inboxTableDescriptor);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (admin != null) {
        try {
          admin.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * 发布微博 <br>
   * 1. 微博内容表中数据+1<br>
   * 2. 向微博收件信箱表中加入微博的Rowkey
   *
   * @param uid
   * @param content
   */
  public void publishContent(String uid, String content) {
    Connection connection = null;
    try {
      connection = ConnectionFactory.createConnection(conf);

      Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));

      long timestamp = System.currentTimeMillis();
      String rowKey = uid + DELIMITER_UNDERLINE + timestamp;

      // 添加微博内容
      Put put = new Put(Bytes.toBytes(rowKey));
      put.addColumn(
          Bytes.toBytes(COLUMN_FAMILY_INFO),
          Bytes.toBytes(QUALIFIER_CONTENT),
          timestamp,
          Bytes.toBytes(content));
      contentTable.put(put);

      // b、向微博收件箱表中加入发布的Rowkey
      // b.1、查询用户关系表，得到当前用户有哪些粉丝
      Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
      // b.2、取出目标数据
      Get get = new Get(Bytes.toBytes(uid));
      get.addFamily(Bytes.toBytes(COLUMN_FAMILY_FANS));

      Result result = relationTable.get(get);
      // 如果该用户没有粉丝，则直接return
      if (result.isEmpty()) return;

      ArrayList<byte[]> fans = new ArrayList<>();

      // 遍历取出当前发布微博的用户的所有粉丝数据
      for (Cell cell : result.listCells()) {
        fans.add(CellUtil.cloneQualifier(cell));
      }

      // 开始操作收件箱表
      Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));

      // 每一个粉丝，都要向收件箱中添加该微博的内容，所以每一个粉丝都是一个Put对象
      List<Put> puts = new ArrayList<>();

      for (byte[] fan : fans) {
        Put fansPut = new Put(fan);
        fansPut.addColumn(
            Bytes.toBytes(COLUMN_FAMILY_INFO),
            Bytes.toBytes(uid),
            timestamp,
            Bytes.toBytes(rowKey));

        puts.add(fansPut);
      }

      inboxTable.put(puts);

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * 获取某一个用户的所有微博
   *
   * @param uid
   * @return
   */
  public List<WeiBoMessage> getUserContent(String uid) {
    // 参数过滤
    if (StringUtils.isEmpty(uid)) {
      return new ArrayList<>();
    }
    Connection connection = null;

    try {
      connection = ConnectionFactory.createConnection(conf);

      // c.1、微博收件箱添加关注的用户发布的微博内容（content）的rowkey
      Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
      Scan scan = new Scan();

      List<WeiBoMessage> messageList = new ArrayList<>();
      // 过滤扫描rowkey，即：前置位匹配被关注的人的uid_
      RowFilter filter =
          new RowFilter(CompareOp.EQUAL, new SubstringComparator(uid + DELIMITER_UNDERLINE));
      // 为扫描对象指定过滤规则
      scan.setFilter(filter);
      // 通过扫描对象得到scanner
      ResultScanner result = contentTable.getScanner(scan);
      // 迭代器遍历扫描出来的结果集
      Iterator<Result> iterator = result.iterator();
      while (iterator.hasNext()) {
        // 取出每一个符合扫描结果的那一行数据
        Result r = iterator.next();
        for (Cell cell : r.listCells()) {
          String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
          String userid = rowKey.substring(0, rowKey.indexOf(DELIMITER_UNDERLINE));
          Long timestamp =
              Long.parseLong(rowKey.substring(rowKey.indexOf(DELIMITER_UNDERLINE) + 1));

          String content = Bytes.toString(CellUtil.cloneValue(cell));
          WeiBoMessage message = new WeiBoMessage(userid, timestamp, content);
          messageList.add(message);
        }
      }

      return messageList;

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return new ArrayList<>();
  }

  /**
   * 关注用户<br>
   * 1. 在微博用户关系表中,对当前主动操作的用户添加新的关注的好友<br>
   * 2. 在微博用户关系表中,对被关注的用户添加粉丝(当前操作的用户)<br>
   * 3. 当前操作用户的微博收件箱添加所关注的用户发布的微博的rowkey(下次用户登录时可以看到关注的人的微博)
   *
   * @param uid
   * @param attends
   */
  public void addAttends(String uid, List<String> attends) {
    // 参数过滤
    if (StringUtils.isEmpty(uid) || CollectionUtils.isEmpty(attends)) {
      return;
    }
    Connection connection = null;

    try {
      connection = ConnectionFactory.createConnection(conf);

      // 用户关系表操作对象（连接到用户关系表）
      Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
      List<Put> puts = new ArrayList<>();
      // a、在微博用户关系表中，添加新关注的好友
      Put attendPut = new Put(Bytes.toBytes(uid));
      for (String attend : attends) {
        // 为当前用户添加关注的人
        attendPut.addColumn(
            Bytes.toBytes(COLUMN_FAMILY_ATTENDS), Bytes.toBytes(attend), Bytes.toBytes(attend));
        // b、为被关注的人，添加粉丝
        Put fansPut = new Put(Bytes.toBytes(attend));
        fansPut.addColumn(
            Bytes.toBytes(COLUMN_FAMILY_FANS), Bytes.toBytes(uid), Bytes.toBytes(uid));
        puts.add(fansPut);
      }

      puts.add(attendPut);
      relationTable.put(puts);

      // c.1、微博收件箱添加关注的用户发布的微博内容（content）的rowkey
      Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
      Scan scan = new Scan();
      List<byte[]> rowkeys = new ArrayList<>();

      for (String attend : attends) {
        // 过滤扫描rowkey，即：前置位匹配被关注的人的uid_
        RowFilter filter =
            new RowFilter(CompareOp.EQUAL, new SubstringComparator(attend + DELIMITER_UNDERLINE));
        // 为扫描对象指定过滤规则
        scan.setFilter(filter);
        // 通过扫描对象得到scanner
        ResultScanner result = contentTable.getScanner(scan);
        // 迭代器遍历扫描出来的结果集
        Iterator<Result> iterator = result.iterator();
        while (iterator.hasNext()) {
          // 取出每一个符合扫描结果的那一行数据
          Result r = iterator.next();
          for (Cell cell : r.listCells()) {
            // 将得到的rowkey放置于集合容器中
            rowkeys.add(CellUtil.cloneRow(cell));
          }
        }
      }

      // c.2、将取出的微博rowkey放置于当前操作的用户的收件箱中
      if (rowkeys.size() <= 0) return;
      Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));

      // 用于存放多个关注的用户的发布的多条微博rowkey信息
      List<Put> inboxPutList = new ArrayList<>();

      for (byte[] rowKey : rowkeys) {
        Put put = new Put(Bytes.toBytes(uid));
        // uid_timestamp
        String rowkeyStr = Bytes.toString(rowKey);
        // 截取uid
        String attendUID = rowkeyStr.substring(0, rowkeyStr.indexOf(DELIMITER_UNDERLINE));
        long timestamp =
            Long.parseLong(rowkeyStr.substring(rowkeyStr.indexOf(DELIMITER_UNDERLINE) + 1));
        // 将微博rowkey添加到指定单元格中
        put.addColumn(
            Bytes.toBytes(COLUMN_FAMILY_INFO), Bytes.toBytes(attendUID), timestamp, rowKey);

        inboxPutList.add(put);
      }

      inboxTable.put(inboxPutList);

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * 取消关注<br>
   * 1. 在微博用户关系表中,对当前主动操作的用户删除对应取关的好友<br>
   * 2. 在微博用户关系表中,对被取消关注的人删除粉丝(当前操作人) <br>
   * 3. 从收件箱中,删除取关的人的微博的rowkey
   *
   * @param uid
   * @param attends
   */
  public void removeAttends(String uid, List<String> attends) {
    // 参数过滤
    if (StringUtils.isEmpty(uid) || CollectionUtils.isEmpty(attends)) {
      return;
    }
    Connection connection = null;

    try {
      connection = ConnectionFactory.createConnection(conf);
      // a、在微博用户关系表中，删除已关注的好友
      Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
      // 待删除的用户关系表中的所有数据
      List<Delete> deleteList = new ArrayList<>();
      // 当前取关操作者的uid对应的Delete对象
      Delete attendDelete = new Delete(Bytes.toBytes(uid));
      // 遍历取关，同时每次取关都要将被取关的人的粉丝-1
      for (String attend : attends) {
        attendDelete.addColumn(Bytes.toBytes(COLUMN_FAMILY_ATTENDS), Bytes.toBytes(attend));

        // b、在微博用户关系表中，对被取消关注的人删除粉丝（当前操作人）
        Delete fansDelete = new Delete(Bytes.toBytes(attend));
        fansDelete.addColumn(Bytes.toBytes(COLUMN_FAMILY_FANS), Bytes.toBytes(uid));
        deleteList.add(fansDelete);
      }
      deleteList.add(attendDelete);
      relationTable.delete(deleteList);

      // c、删除取关的人的微博rowkey 从 收件箱表中
      Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
      Delete inboxDelete = new Delete(Bytes.toBytes(uid));

      for (String attend : attends) {
        inboxDelete.addColumn(Bytes.toBytes(COLUMN_FAMILY_INFO), Bytes.toBytes(attend));
      }
      inboxTable.delete(inboxDelete);

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * 获取微博实际内容<br>
   * a、从微博收件箱中获取所有关注的人的发布的微博的rowkey<br>
   * b、根据得到的rowkey去微博内容表中得到数据<br>
   * c、将得到的数据封装到Message对象中<br>
   *
   * @param uid
   * @return
   */
  public List<WeiBoMessage> getAttendsContent(String uid) {
    // 参数过滤
    if (StringUtils.isEmpty(uid)) {
      return new ArrayList<>();
    }
    Connection connection = null;

    try {
      connection = ConnectionFactory.createConnection(conf);
      Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));

      Get get = new Get(Bytes.toBytes(uid));
      get.setMaxVersions(WEIBO_INBOX_GET_MAX_VERSION);
      List<byte[]> rowKeys = new ArrayList<>();
      Result result = inboxTable.get(get);
      // 如果获取的结果为空
      if (result.isEmpty()) return new ArrayList<>();

      for (Cell cell : result.listCells()) {
        rowKeys.add(CellUtil.cloneValue(cell));
      }

      // b、根据取出的所有rowkey去微博内容表中检索数据
      Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
      List<Get> gets = new ArrayList<>();

      for (byte[] rowKey : rowKeys) {
        Get g = new Get(rowKey);
        gets.add(g);
      }

      Result[] results = contentTable.get(gets);

      List<WeiBoMessage> messageList = new ArrayList<>();

      for (Result res : results) {
        for (Cell cell : res.listCells()) {

          String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
          String userid = rowKey.substring(0, rowKey.indexOf(DELIMITER_UNDERLINE));
          Long timestamp =
              Long.parseLong(rowKey.substring(rowKey.indexOf(DELIMITER_UNDERLINE) + 1));

          String content = Bytes.toString(CellUtil.cloneValue(cell));
          WeiBoMessage message = new WeiBoMessage(userid, timestamp, content);
          messageList.add(message);
        }
      }

      return messageList;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return new ArrayList<>();
  }
}
