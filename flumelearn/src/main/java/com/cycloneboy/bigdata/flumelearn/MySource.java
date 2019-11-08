package com.cycloneboy.bigdata.flumelearn;

import java.util.HashMap;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * Create by sl on 2019-11-08 21:17 <br>
 * 使用flume接收数据，并给每条数据添加前缀，输出到控制台。前缀可从flume配置文件中配置。
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

  // 定义配置文件将来要读取的字段
  private Long delay;
  private String field;

  /** 初始化配置信息 */
  @Override
  public void configure(Context context) {
    delay = context.getLong("delay");
    field = context.getString("field", "Hello!");
  }

  @Override
  public Status process() throws EventDeliveryException {
    try {
      // 创建事件头信息
      HashMap<String, String> hearderMap = new HashMap<>();

      // 创建事件
      SimpleEvent event = new SimpleEvent();

      // 循环封装事件
      for (int i = 0; i < 5; i++) {
        // 给事件设置头信息
        event.setHeaders(hearderMap);
        // 给事件设置内容
        event.setBody((field + i).getBytes());
        // 将事件写入channel
        getChannelProcessor().processEvent(event);

        Thread.sleep(delay);
      }

    } catch (Exception e) {
      e.printStackTrace();
      return Status.BACKOFF;
    }
    return Status.READY;
  }

  @Override
  public long getBackOffSleepIncrement() {
    return 0;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return 0;
  }
}
