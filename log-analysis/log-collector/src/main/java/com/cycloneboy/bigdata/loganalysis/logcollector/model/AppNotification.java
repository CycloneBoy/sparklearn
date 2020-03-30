package com.cycloneboy.bigdata.loganalysis.logcollector.model;

import lombok.Data;

/** Create by sl on 2020-03-16 21:18 */
@Data
public class AppNotification {

  private String action; // 动作：通知产生=1，通知弹出=2，通知点击=3，常驻通知展示（不重复上报，一天之内只报一次）=4
  private String type; // 通知id：预警通知=1，天气预报（早=2，晚=3），常驻=4
  private String ap_time; // 客户端弹出时间
  private String content; // 备用字段
}
