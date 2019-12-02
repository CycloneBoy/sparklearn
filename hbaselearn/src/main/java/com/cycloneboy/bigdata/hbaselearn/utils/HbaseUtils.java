package com.cycloneboy.bigdata.hbaselearn.utils;

import static com.cycloneboy.bigdata.hbaselearn.common.Constants.TABLE_STUDENT2;

import java.io.IOException;
import java.util.ArrayList;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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

  static {
    // 使用HBaseConfiguration的单例方法实例化
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
  }

  /**
   * 判断表是否存在
   *
   * @param tableName
   * @return
   */
  public static boolean isTableExist(String tableName) throws IOException {
    // 在HBase中管理、访问表需要先创建HBaseAdmin对象
    //    Connection connection = ConnectionFactory.createConnection(conf);
    //    HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
    HBaseAdmin admin = new HBaseAdmin(conf);

    return admin.tableExists(tableName);
  }

  /**
   * 创建表
   *
   * @param tableName
   * @param columnFamily
   * @throws IOException
   */
  public static void createTable(String tableName, String... columnFamily) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    // 判断表是否存在
    if (isTableExist(tableName)) {
      log.info("表 {} 已经存在 ", tableName);
    } else {
      // 创建表属性对象,表名需要转字节
      HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
      // 创建多个列族
      for (String cf : columnFamily) {
        descriptor.addFamily(new HColumnDescriptor(cf));
      }
      // 根据对表的配置，创建表
      admin.createTable(descriptor);
      log.info("表 {} 创建成功! ", tableName);
    }
  }

  /**
   * 删除表格
   *
   * @param tableName
   * @throws IOException
   */
  public static void dropTable(String tableName) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);

    if (isTableExist(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      log.info("表 {} 删除成功! ", tableName);
    } else {
      log.info("表 {} 不存在!", tableName);
    }
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
    log.info("{} 是否存在 {}", TABLE_STUDENT2, HbaseUtils.isTableExist(TABLE_STUDENT2));
  }
}
