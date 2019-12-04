package com.cycloneboy.bigdata.communication.hbase;

import com.cycloneboy.bigdata.communication.common.Constants;
import com.cycloneboy.bigdata.communication.conf.ConfigurationManager;
import com.cycloneboy.bigdata.communication.model.CallLog;
import com.cycloneboy.bigdata.communication.utils.HbaseUtils;
import java.io.IOException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

/** Create by sl on 2019-12-04 19:13 */
public class CalleeWriteObserver extends BaseRegionObserver {

  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  @Override
  public void postPut(
      ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
      throws IOException {
    // 1、获取你想要操作的目标表的名称
    String tableName =
        ConfigurationManager.config().getString(Constants.CONF_HBASE_CALLLOG_TABLENAME());

    String callerFlag =
        ConfigurationManager.config().getString(Constants.CONF_HBASE_CALLLOG_CALLER_FLAG());

    String calleeFlag =
        ConfigurationManager.config().getString(Constants.CONF_HBASE_CALLLOG_CALLEE_FLAG());

    int regions = ConfigurationManager.config().getInt(Constants.CONF_HBASE_CALLLOG_REGIONS());

    String calleeFamily =
        ConfigurationManager.config().getString(Constants.CONF_HBASE_CALLLOG_CALLEE_FAMILY());

    // 2、获取当前成功Put了数据的表（不一定是我们当前业务想要操作的表）

    String currentTableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();

    if (!tableName.equals(currentTableName)) return;

    // 01_158373123456_20170110154530_13737312345_1_0360
    String oriRowKey = Bytes.toString(put.getRow());

    String[] splitOriRowKey = oriRowKey.split("_");
    String oldFlag = splitOriRowKey[4];

    // 如果当前插入的是被叫数据，则直接返回(因为默认提供的数据全部为主叫数据)
    if (callerFlag.equals(oldFlag)) return;

    CallLog callLog = new CallLog(oriRowKey);
    callLog.setFlag(calleeFlag);

    String regionCode =
        HbaseUtils.getRegionCode(callLog.getCaller(), callLog.getBuildTime(), regions);

    // 生成rowkey
    String rowKey = HbaseUtils.genRowKey(regionCode, callLog);

    Put calleePut = new Put(Bytes.toBytes(rowKey));

    calleePut.addColumn(
        Bytes.toBytes(calleeFamily), Bytes.toBytes("caller"), Bytes.toBytes(callLog.getCaller()));
    calleePut.addColumn(
        Bytes.toBytes(calleeFamily), Bytes.toBytes("callee"), Bytes.toBytes(callLog.getCallee()));
    calleePut.addColumn(
        Bytes.toBytes(calleeFamily),
        Bytes.toBytes("buildTime"),
        Bytes.toBytes(callLog.getBuildTime()));
    calleePut.addColumn(
        Bytes.toBytes(calleeFamily),
        Bytes.toBytes("buildTimeTs"),
        Bytes.toBytes(callLog.getBuildTimeTs()));
    calleePut.addColumn(
        Bytes.toBytes(calleeFamily), Bytes.toBytes("flag"), Bytes.toBytes(callLog.getFlag()));
    calleePut.addColumn(
        Bytes.toBytes(calleeFamily),
        Bytes.toBytes("duration"),
        Bytes.toBytes(callLog.getDuration()));

    Table table = e.getEnvironment().getTable(TableName.valueOf(tableName));

    table.put(calleePut);

    table.close();
  }
}
