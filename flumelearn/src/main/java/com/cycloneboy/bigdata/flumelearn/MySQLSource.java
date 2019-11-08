package com.cycloneboy.bigdata.flumelearn;

import com.cycloneboy.bigdata.flumelearn.utils.SQLSourceHelper;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.naming.ConfigurationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * Create by sl on 2019-11-08 22:17 <br>
 * 实时监控MySQL，从MySQL中获取数据传输到HDFS或者其他存储框架，所以此时需要我们自己实现MySQLSource。
 */
@Slf4j
public class MySQLSource extends AbstractSource implements Configurable, PollableSource {

  // 定义sqlHelper
  private SQLSourceHelper sqlSourceHelper;

  @Override
  public long getBackOffSleepIncrement() {
    return 0;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return 0;
  }

  @Override
  public void configure(Context context) {
    try {
      // 初始化
      sqlSourceHelper = new SQLSourceHelper(context);
    } catch (ParseException | ConfigurationException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    try {
      // 查询数据表
      List<List<Object>> result = sqlSourceHelper.executeQuery();
      // 存放event的集合
      List<Event> events = new ArrayList<>();
      // 存放event头集合
      HashMap<String, String> header = new HashMap<>();
      // 如果有返回数据，则将数据封装为event
      if (!result.isEmpty()) {
        List<String> allRows = sqlSourceHelper.getAllRows(result);
        Event event = null;
        for (String row : allRows) {
          event = new SimpleEvent();
          event.setBody(row.getBytes());
          event.setHeaders(header);
          events.add(event);
        }
        // 将event写入channel
        this.getChannelProcessor().processEventBatch(events);
        // 更新数据表中的offset信息
        sqlSourceHelper.updateOffset2DB(result.size());
      }
      // 等待时长
      Thread.sleep(sqlSourceHelper.getRunQueryDelay());
      return Status.READY;
    } catch (InterruptedException e) {
      log.error("Error procesing row", e);
      return Status.BACKOFF;
    }
  }

  @Override
  public synchronized void stop() {
    log.info("Stopping sql source {} ...", getName());
    try {
      // 关闭资源
      sqlSourceHelper.close();
    } finally {
      super.stop();
    }
  }
}
