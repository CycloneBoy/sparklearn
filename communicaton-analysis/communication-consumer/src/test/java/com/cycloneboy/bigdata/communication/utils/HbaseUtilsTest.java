package com.cycloneboy.bigdata.communication.utils;

import com.cycloneboy.bigdata.communication.common.Constants;
import com.cycloneboy.bigdata.communication.conf.ConfigurationManager;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

/** Create by sl on 2019-12-04 15:57 */
@Slf4j
public class HbaseUtilsTest {

  String tableName = "call:calllog";

  String callerFamily =
      ConfigurationManager.config().getString(Constants.CONF_HBASE_CALLLOG_CALLER_FAMILY());

  @Test
  public void isTableExist() throws IOException {
    boolean result = false;

    HBaseAdmin admin = new HBaseAdmin(HbaseUtils.conf);

    result = admin.tableExists(tableName);
    log.info("{}", result);

    //    result = HbaseUtils.isTableExist("call:calllog");
    log.info("{}", result);
  }

  @Test
  public void initNamespace() throws IOException {
    HbaseUtils.dropTable(tableName);

    //    HbaseUtils.createTable(tableName, 6, Arrays.asList(callerFamily.split(",")));
  }

  @Test
  public void createTable() {}
}
