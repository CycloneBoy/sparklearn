package com.cycloneboy.bigdata.loganalysis.logcollector.model;

import lombok.Data;

/** Create by sl on 2020-03-16 21:15 事件日志Bean之商品点击 */
@Data
public class AppDisplay {

  private String action; // 动作：曝光商品=1，点击商品=2，
  private String goodsid; // 商品ID（服务端下发的ID）
  private String place; // 顺序（第几条商品，第一条为0，第二条为1，如此类推）
  private String extend1; // 曝光类型：1 - 首次曝光 2-重复曝光（没有使用）
  private String category; // 分类ID（服务端定义的分类ID）
}
