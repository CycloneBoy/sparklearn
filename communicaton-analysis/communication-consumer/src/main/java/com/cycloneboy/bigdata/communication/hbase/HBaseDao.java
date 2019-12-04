package com.cycloneboy.bigdata.communication.hbase;

import com.cycloneboy.bigdata.communication.annocation.Column;
import com.cycloneboy.bigdata.communication.annocation.RowKey;
import com.cycloneboy.bigdata.communication.annocation.TableRef;
import com.cycloneboy.bigdata.communication.common.Constants;
import com.cycloneboy.bigdata.communication.conf.ConfigurationManager;
import com.cycloneboy.bigdata.communication.model.CallLog;
import com.cycloneboy.bigdata.communication.utils.DateUtils;
import com.cycloneboy.bigdata.communication.utils.HBaseConnectionInstance;
import com.cycloneboy.bigdata.communication.utils.HbaseUtils;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/** Create by sl on 2019-12-04 14:51 */
@Slf4j
public class HBaseDao {

  private static int regions;
  private static String namespace;
  private static String tableName;
  private static String columnFamily;
  private static String callerFamily;

  public static Configuration conf;
  private Table table;
  private Connection connection;
  private List<Put> cacheList = new ArrayList<>();

  static {
    // 使用HBaseConfiguration的单例方法实例化
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");

    namespace = ConfigurationManager.config().getString(Constants.CONF_HBASE_CALLLOG_NAMESPACE());
    tableName = ConfigurationManager.config().getString(Constants.CONF_HBASE_CALLLOG_TABLENAME());
    regions = ConfigurationManager.config().getInt(Constants.CONF_HBASE_CALLLOG_REGIONS());
    columnFamily = ConfigurationManager.config().getString(Constants.CONF_HBASE_CALLLOG_FAMILIES());
    callerFamily =
        ConfigurationManager.config().getString(Constants.CONF_HBASE_CALLLOG_CALLER_FAMILY());
  }

  public HBaseDao() {
    try {
      /** 创建命名空间和表名称 */
      if (!HbaseUtils.isTableExist(tableName)) {
        HbaseUtils.initNamespace(namespace);
        HbaseUtils.createTable(tableName, regions, Arrays.asList(columnFamily.split(",")));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 获取连接
   *
   * @return
   */
  private Connection getConnection() {
    connection = HBaseConnectionInstance.getConnection(conf);
    return connection;
  }

  /**
   * 保存通话话单<br>
   *
   * @param callLog 原始通话话单 形式：15837312345,13737312345,2017-01-09 08:09:10,0360
   */
  public void put(String callLog) {
    put(new CallLog(callLog));
  }

  public void putData(Object obj) throws IOException {

    Class clasz = obj.getClass();
    TableRef tableRef = (TableRef) clasz.getAnnotation(TableRef.class);

    // 获取表名称
    String tableName = tableRef.value();
    String rowkeyStr = "";

    Field[] fields = clasz.getDeclaredFields();
    // 获取rowkey
    for (Field field : fields) {
      RowKey rowKey = field.getAnnotation(RowKey.class);
      if (rowKey != null) {
        field.setAccessible(true);
        try {
          rowkeyStr = (String) field.get(obj);
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
        break;
      }
    }

    Put put = new Put(Bytes.toBytes(rowkeyStr));

    for (Field field : fields) {
      Column column = field.getAnnotation(Column.class);
      if (column == null) {
        continue;
      }

      String family = column.family();
      String colName = column.column();
      if (StringUtils.isEmpty(colName)) {
        colName = field.getName();
      }

      field.setAccessible(true);
      String value = "";
      try {
        value = String.valueOf(field.get(obj));
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }

      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName), Bytes.toBytes(value));
    }

    Table table = getConnection().getTable(TableName.valueOf(tableName));
    table.put(put);

    table.close();
  }

  /**
   * 保存通话话单<br>
   * ori数据样式： 18576581848,17269452013,2017-08-14 13:38:31,1761 <br>
   * rowkey样式：01_18576581848_20170814133831_17269452013_1_1761 <br>
   * HBase表的列：call1 call2 build_time build_time_ts flag duration
   *
   * @param callLog
   */
  public void put(CallLog callLog) {
    put(callerFamily, callLog);
  }

  /**
   * 保存通话话单<br>
   * ori数据样式： 18576581848,17269452013,2017-08-14 13:38:31,1761 <br>
   * rowkey样式：01_18576581848_20170814133831_17269452013_1_1761 <br>
   * HBase表的列：call1 call2 build_time build_time_ts flag duration
   *
   * @param colunFamily 列族
   * @param callLog 通话话单
   */
  public void put(String colunFamily, CallLog callLog) {
    try {
      if (cacheList.size() == 0) {

        table = getConnection().getTable(TableName.valueOf(tableName));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    String regionCode =
        HbaseUtils.getRegionCode(callLog.getCaller(), callLog.getBuildTime(), regions);

    // 生成rowkey
    String rowKey = HbaseUtils.genRowKey(regionCode, callLog);

    Put put = new Put(Bytes.toBytes(rowKey));
    put.addColumn(
        Bytes.toBytes(colunFamily), Bytes.toBytes("caller"), Bytes.toBytes(callLog.getCaller()));
    put.addColumn(
        Bytes.toBytes(colunFamily), Bytes.toBytes("callee"), Bytes.toBytes(callLog.getCallee()));
    put.addColumn(
        Bytes.toBytes(colunFamily),
        Bytes.toBytes("buildTime"),
        Bytes.toBytes(callLog.getBuildTime()));
    put.addColumn(
        Bytes.toBytes(colunFamily),
        Bytes.toBytes("buildTimeTs"),
        Bytes.toBytes(callLog.getBuildTimeTs()));
    put.addColumn(
        Bytes.toBytes(colunFamily), Bytes.toBytes("flag"), Bytes.toBytes(callLog.getFlag()));
    put.addColumn(
        Bytes.toBytes(colunFamily),
        Bytes.toBytes("duration"),
        Bytes.toBytes(callLog.getDuration()));

    cacheList.add(put);
    try {
      if (cacheList.size() >= 30) {
        table.put(cacheList);
        table.close();
        cacheList.clear();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 获取查询时间范围内的startRow和stopRow
   *
   * @param phone
   * @param start yyyyMM
   * @param end
   * @return
   */
  public List<String[]> getStartStopRowkeys(String phone, String start, String end) {
    List<String[]> rowkeyList = new ArrayList<>();

    String startTime = start.substring(0, 6);
    String endTime = end.substring(0, 6);

    Calendar startCalendar = Calendar.getInstance();
    startCalendar.setTime(DateUtils.parse(startTime, "yyyyMM"));

    Calendar endCalendar = Calendar.getInstance();
    endCalendar.setTime(DateUtils.parse(endTime, "yyyyMM"));

    while (startCalendar.getTimeInMillis() <= endCalendar.getTimeInMillis()) {
      // 当前时间
      String nowTime = DateUtils.formatDateTime(startCalendar.getTime(), "yyyyMM");

      String regionCode = HbaseUtils.getRegionCode(phone, nowTime, regions);
      String startRow = regionCode + "_" + phone + "_" + nowTime;
      String endRow = startRow + "|";
      String[] rowkey = {startRow, endRow};
      rowkeyList.add(rowkey);

      // 月份+1
      startCalendar.add(Calendar.MONTH, 1);
    }

    return rowkeyList;
  }
}
