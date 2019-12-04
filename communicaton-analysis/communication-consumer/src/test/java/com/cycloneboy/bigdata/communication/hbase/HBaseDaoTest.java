package com.cycloneboy.bigdata.communication.hbase;

import com.cycloneboy.bigdata.communication.annocation.RowKey;
import com.cycloneboy.bigdata.communication.annocation.TableRef;
import com.cycloneboy.bigdata.communication.model.CallLog;
import com.cycloneboy.bigdata.communication.utils.HbaseUtils;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

/** Create by sl on 2019-12-04 15:58 */
@Slf4j
public class HBaseDaoTest {

  private HBaseDao hBaseDao;

  @Before
  public void setup() {
    hBaseDao = new HBaseDao();
  }

  @Test
  public void init() {}

  @Test
  public void put() throws IOException, IllegalAccessException {
    HBaseDao hBaseDao = new HBaseDao();
    CallLog callLog = new CallLog("18468618874,15542823911,2019-07-11 21:03:47,0712");
    callLog.setRowkey(HbaseUtils.genRowKey("00", callLog));
    log.info(callLog.toString());
    hBaseDao.putData(callLog);

    //    callLog.setRowkey(HbaseUtils.genRowKey("01", callLog));
    //
    //    callLog.setRowkey(HbaseUtils.genRowKey("02", callLog));
    //    callLog.setRowkey(HbaseUtils.genRowKey("03", callLog));
    //    callLog.setRowkey(HbaseUtils.genRowKey("04", callLog));
    //    callLog.setRowkey(HbaseUtils.genRowKey("05", callLog));

    //    log.info(callLog.toString());
    //    hBaseDao.putData(callLog);
  }

  @Test
  public void testRef() {
    CallLog obj = new CallLog("18468618874,15542823911,2019-07-11 21:03:47,0712");
    obj.setRowkey(HbaseUtils.genRowKey("01", obj));

    Class clasz = obj.getClass();
    TableRef tableRef = (TableRef) clasz.getAnnotation(TableRef.class);

    // 获取表名称
    String tableName = tableRef.value();
    String rowkeyStr = "";

    Field[] fields = clasz.getDeclaredFields();
    System.out.println("--" + obj.getClass().getDeclaredFields().length);
    // 获取rowkey
    for (Field field : fields) {
      System.out.println("--");
      log.info("***********field**************");
      RowKey rowKey = field.getAnnotation(RowKey.class);
      if (rowKey != null) {
        log.info("------------------------------------>rowKey != null");
        field.setAccessible(true);
        try {
          rowkeyStr = (String) field.get(obj);
          log.info("rowkey is :{}", rowkeyStr);
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
        break;
      }

      log.info("------------------------------------>rowKey == null");
    }
  }

  @Test
  public void testGetRow() {
    List<String[]> startStopRowkeys =
        hBaseDao.getStartStopRowkeys("13018001222", "201810", "201903");

    startStopRowkeys.forEach(row -> log.info("{} - {}", row[0], row[1]));
  }
}
