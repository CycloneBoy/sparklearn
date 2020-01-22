package com.cycloneboy.bigdata.stormlearn.common.log;

import lombok.Data;

/** Create by sl on 2020-01-14 18:48 */
@Data
public class ViewLog implements Comparable<ViewLog> {

  private int clickId;

  private int clickType;

  private int orderId;

  private int orderType;

  /** 1:学生 2:数码产品高频购买者 3:家庭主人 4: 白领 5: 吃货 */
  private int type;

  /** 两个数据距离 */
  private double distance;

  @Override
  public int compareTo(ViewLog o) {
    if (this.distance < o.getDistance()) {
      return -1;
    } else if (this.distance > o.getDistance()) {
      return 1;
    }
    return 0;
  }
}
