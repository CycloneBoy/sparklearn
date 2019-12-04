package com.cycloneboy.bigdata.communication.utils;

import com.cycloneboy.bigdata.communication.common.Constants;
import com.cycloneboy.bigdata.communication.conf.ConfigurationManager;
import com.cycloneboy.bigdata.communication.model.CallLog;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/** Create by sl on 2019-12-02 12:54 */
@Slf4j
public class HbaseUtils {

  public static Configuration conf;

  private static DecimalFormat regionCodeFormat = null;

  static {
    // 使用HBaseConfiguration的单例方法实例化
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");

    regionCodeFormat =
        new DecimalFormat(
            ConfigurationManager.config()
                .getString(Constants.CONF_HBASE_CALLLOG_REGIONCODE_FORMAT()));
  }

  /**
   * 判断表是否存在
   *
   * @param tableName
   * @return
   */
  public static boolean isTableExist(String tableName) throws IOException {
    // 在HBase中管理、访问表需要先创建HBaseAdmin对象
    Connection connection = ConnectionFactory.createConnection(conf);

    Admin admin = connection.getAdmin();

    boolean result = admin.tableExists(TableName.valueOf(tableName));

    admin.close();
    connection.close();
    return result;
  }

  /** 初始化命名空间 */
  public static void initNamespace(String namespace) {
    Admin admin = null;
    try {

      admin = HBaseConnectionInstance.getConnection(conf).getAdmin();
      // 判断是否存在此namespace
      try {
        admin.getNamespaceDescriptor(namespace);
        log.info("命名空间:{}已存在", namespace);
      } catch (NamespaceNotFoundException e) {
        e.printStackTrace();

        NamespaceDescriptor nd =
            NamespaceDescriptor.create(namespace)
                .addConfiguration("creator", "sl")
                .addConfiguration("create_time", System.currentTimeMillis() + "")
                .build();

        admin.createNamespace(nd);
        log.info("初始化命名空间:{}", namespace);
      }

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
   * 创建表<br>
   * 默认一个分区
   *
   * @param tableName hbase 表名称
   * @param columnFamily hbase 列族信息
   * @throws IOException
   */
  public static void createTable(String tableName, List<String> columnFamily) throws IOException {
    createTable(tableName, 1, columnFamily);
  }

  /**
   * 创建表 添加协处理器
   *
   * @param tableName hbase 表名称
   * @param regions 分区数量
   * @param columnFamily hbase 列族信息
   * @throws IOException
   */
  public static void createTable(String tableName, int regions, List<String> columnFamily)
      throws IOException {
    Admin admin = HBaseConnectionInstance.getConnection(conf).getAdmin();
    // 判断表是否存在
    if (isTableExist(tableName)) {
      log.info("表 {} 已经存在 ", tableName);
      return;
    }
    // 创建表属性对象,表名需要转字节
    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
    // 创建多个列族
    for (String cf : columnFamily) {
      descriptor.addFamily(new HColumnDescriptor(cf));
    }

    // 添加协处理器
    descriptor.addCoprocessor("com.cycloneboy.bigdata.communication.hbase.CalleeWriteObserver");
    // 根据对表的配置，创建表
    admin.createTable(descriptor, getSplitKey(regions));
    log.info("表 {} 创建成功! ", tableName);

    admin.close();
  }

  /**
   * 创建分区键<br>
   * 两位数字分区表
   *
   * @param regions 分区数量
   * @return 分区表
   */
  private static byte[][] getSplitKey(int regions) {
    // 定义一个存放分区键的数组
    int splitCount = regions - 1;
    List<byte[]> bsList = new ArrayList<>();
    // 分区数量和分区键的关系
    byte[][] splitKeys = new byte[splitCount][];
    // 目前推算，region个数不会超过2位数，所以region分区键格式化为两位数字所代表的字符串
    //    DecimalFormat decimalFormat = new DecimalFormat("00");
    for (int i = 0; i < splitCount; i++) {
      // "|" 值比数字大
      String splitkey = regionCodeFormat.format(i) + "|";
      bsList.add(Bytes.toBytes(splitkey));
    }

    // 生成byte[][]类型的分区键的时候，一定要保证分区键是有序的
    bsList.sort(new Bytes.ByteArrayComparator());
    bsList.toArray(splitKeys);
    return splitKeys;
  }

  /**
   * 生成rowkey <br>
   * regionCode_call1_buildTime_call2_flag_duration
   *
   * @param regionCode 分区编号
   * @param callLog 通话话单
   * @return 生成rowkey
   */
  public static String genRowKey(String regionCode, CallLog callLog) {
    StringBuilder sb = new StringBuilder();
    sb.append(regionCode)
        .append(Constants.DELIMITER_UNDERLINE())
        .append(callLog.getCaller())
        .append(Constants.DELIMITER_UNDERLINE())
        .append(callLog.getBuildTime())
        .append(Constants.DELIMITER_UNDERLINE())
        .append(callLog.getCallee())
        .append(Constants.DELIMITER_UNDERLINE())
        .append(callLog.getFlag())
        .append(Constants.DELIMITER_UNDERLINE())
        .append(callLog.getDuration());

    return sb.toString();
  }

  /**
   * 生成分区键 (0,1,2,3,4)<br>
   * 基于手机号前4位,通行时间前6位 取或(特征并集),<br>
   * 然后对分区数取余,格式化为"00" 格式,作为分区标识输出
   *
   * @param caller 手机号：15837312345
   * @param buildTime 通话建立时间： 20170110112030 (格式: yyyyMMddHHmmss)
   * @param regions 分区数量
   * @return 生成分区键
   */
  public static String getRegionCode(String caller, String buildTime, int regions) {
    if (StringUtils.isEmpty(caller) || caller.length() < 4 || regions < 0) return "";
    // 取手机号后四位
    String lastPhone = caller.substring(caller.length() - 4);
    // 取通话时间的前六位 YYYYmm
    String time = buildTime.substring(0, 6);

    return regionCodeFormat.format((lastPhone.hashCode() ^ time.hashCode()) % regions);
  }

  /**
   * 删除表格
   *
   * @param tableName 表名称
   * @throws IOException
   */
  public static void dropTable(String tableName) throws IOException {
    Admin admin = HBaseConnectionInstance.getConnection(conf).getAdmin();

    if (isTableExist(tableName)) {
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
      log.info("表 {} 删除成功! ", tableName);
    } else {
      log.info("表 {} 不存在!", tableName);
    }
    admin.close();
  }

  /**
   * 向表中插入数据
   *
   * @param tableName
   * @param rowKey
   * @param columnFamily
   * @param column
   * @param value
   * @throws IOException
   */
  public static void addRowData(
      String tableName, String rowKey, String columnFamily, String column, String value)
      throws IOException {

    // 创建HTable对象
    HTable hTable = new HTable(conf, tableName);

    // 向表中插入数据
    Put put = new Put(Bytes.toBytes(rowKey));
    // 向Put对象中组装数据
    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

    hTable.put(put);
    hTable.close();

    log.info("表 {} ,{},{} -> {}插入数据成功!", tableName, columnFamily, column, value);
  }

  /**
   * 删除多行数据
   *
   * @param tableName
   * @param rows
   * @throws IOException
   */
  public static void deleteMultiRow(String tableName, String... rows) throws IOException {
    HTable hTable = new HTable(conf, tableName);
    ArrayList<Delete> deleteList = new ArrayList<>();

    for (String row : rows) {
      Delete delete = new Delete(Bytes.toBytes(row));
      deleteList.add(delete);
    }

    hTable.delete(deleteList);
    hTable.close();

    log.info("表 {} 删除多列 {} 成功! ", tableName, rows.toString());
  }

  /**
   * 获取所有数据
   *
   * @param tableName
   * @throws IOException
   */
  public static void getAllRows(String tableName) throws IOException {
    HTable hTable = new HTable(conf, tableName);
    Scan scan = new Scan();

    ResultScanner resultScanner = hTable.getScanner(scan);

    for (Result result : resultScanner) {
      Cell[] cells = result.rawCells();
      for (Cell cell : cells) {
        log.info("行健:{}", Bytes.toString(CellUtil.cloneRow(cell)));
        log.info("列族:{}", Bytes.toString(CellUtil.cloneFamily(cell)));
        log.info("列:{}", Bytes.toString(CellUtil.cloneQualifier(cell)));
        log.info("值:{}", Bytes.toString(CellUtil.cloneValue(cell)));
      }
    }
  }

  /**
   * 获取某一行数据
   *
   * @param tableName
   * @param rowKey
   * @throws IOException
   */
  public static void getRow(String tableName, String rowKey) throws IOException {
    HTable hTable = new HTable(conf, tableName);
    Get get = new Get(Bytes.toBytes(rowKey));
    //    log.info("显示所有版本:{}", get.setMaxVersions());
    //    log.info("显示指定时间戳的版本:{}", get.setTimeStamp());

    Result result = hTable.get(get);

    for (Cell cell : result.listCells()) {
      log.info("行健:{}", Bytes.toString(CellUtil.cloneRow(cell)));
      log.info("列族:{}", Bytes.toString(CellUtil.cloneFamily(cell)));
      log.info("列:{}", Bytes.toString(CellUtil.cloneQualifier(cell)));
      log.info("值:{}", Bytes.toString(CellUtil.cloneValue(cell)));
    }
  }

  /**
   * 获取某一行指定“列族:列”的数据
   *
   * @param tableName
   * @param rowKey
   * @param family
   * @param qualifier
   * @throws IOException
   */
  public static void getRowQualifier(
      String tableName, String rowKey, String family, String qualifier) throws IOException {

    // 创建HTable对象
    HTable hTable = new HTable(conf, tableName);
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

    Result result = hTable.get(get);

    for (Cell cell : result.listCells()) {
      log.info("行健:{}", Bytes.toString(CellUtil.cloneRow(cell)));
      log.info("列族:{}", Bytes.toString(CellUtil.cloneFamily(cell)));
      log.info("列:{}", Bytes.toString(CellUtil.cloneQualifier(cell)));
      log.info("值:{}", Bytes.toString(CellUtil.cloneValue(cell)));
    }
  }

  public static void main(String[] args) throws IOException {
    log.info("test");
    //    log.info("{} 是否存在 {}", TABLE_STUDENT2, HbaseUtils.isTableExist(TABLE_STUDENT2));
  }
}
