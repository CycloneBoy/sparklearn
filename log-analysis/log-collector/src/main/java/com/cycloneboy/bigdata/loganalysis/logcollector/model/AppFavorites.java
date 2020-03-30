package com.cycloneboy.bigdata.loganalysis.logcollector.model;

import lombok.Data;

/** Create by sl on 2020-03-16 21:19 */
@Data
public class AppFavorites {

  private int id; // 主键
  private int course_id; // 商品id
  private int userid; // 用户ID
  private String add_time; // 创建时间
}
