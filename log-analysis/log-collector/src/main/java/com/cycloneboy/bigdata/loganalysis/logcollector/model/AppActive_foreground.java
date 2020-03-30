package com.cycloneboy.bigdata.loganalysis.logcollector.model;

import lombok.Data;

/** Create by sl on 2020-03-16 21:18 */
@Data
public class AppActive_foreground {

  private String push_id; // 推送的消息的id，如果不是从推送消息打开，传空
  private String access; // 1.push 2.icon 3.其他
}
