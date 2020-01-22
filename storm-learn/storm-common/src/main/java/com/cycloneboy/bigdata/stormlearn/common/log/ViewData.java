package com.cycloneboy.bigdata.stormlearn.common.log;

import lombok.Data;

/** Create by sl on 2020-01-14 20:24 */
@Data
public class ViewData {

  private long time;

  private int userId;

  private String sessionId;

  private int pageId;

  private int clickId;

  private int clickType;

  private int orderId;

  private int orderType;

  /** 1:学生 2:数码产品高频购买者 3:家庭主人 4: 白领 5: 吃货 */
  private int type;

  /**
   * 动作日期 用户ID SessionId 页面ID 点击产品ID 点击类别ID 订单产品ID 订单类别ID 类别 123 10001-100020 20001-20100 1-20 1-100
   * 1-20 1-100 1
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(time);
    sb.append("\t").append(userId);
    sb.append("\t").append(sessionId);
    sb.append("\t").append(pageId);
    sb.append("\t").append(clickId);
    sb.append("\t").append(clickType);
    sb.append("\t").append(orderId);
    sb.append("\t").append(orderType);
    sb.append("\t").append(type);
    return sb.toString();
  }
}
