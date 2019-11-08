package com.cycloneboy.bigdata.flumelearn;

import lombok.extern.slf4j.Slf4j;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * Create by sl on 2019-11-08 21:35 <br>
 * 使用flume接收数据，并在Sink端给每条数据添加前缀和后缀，输出到控制台。前后缀可在flume任务配置文件中配置。
 */
@Slf4j
public class MySink extends AbstractSink implements Configurable {
  private String prefix;
  private String suffix;

  @Override
  public Status process() throws EventDeliveryException {
    // 申明返回值状态信息
    Status status;

    // 获取当前Sink绑定的Channel
    Channel ch = getChannel();

    // 获取事务
    Transaction txn = ch.getTransaction();

    // 申明事件
    Event event;

    // 开启事务
    txn.begin();

    // 读取Channel 中的事件,直到读取到事件循环结束
    while (true) {
      event = ch.take();
      if (event != null) {
        break;
      }
    }

    try {

      // 处理事件(打印)
      log.info("{} - {} - {}", prefix, new String(event.getBody()), suffix);

      // 事务提交
      txn.commit();
      status = Status.READY;
    } catch (Exception e) {
      // 遇到异常,事务回滚
      txn.rollback();
      status = Status.BACKOFF;
    } finally {
      // 关闭事务
      txn.close();
    }
    return status;
  }

  @Override
  public void configure(Context context) {

    // 读取配置文件内容，有默认值
    prefix = context.getString("prefix", "hello");

    // 读取配置文件内容，无默认值
    suffix = context.getString("suffix");
  }
}
