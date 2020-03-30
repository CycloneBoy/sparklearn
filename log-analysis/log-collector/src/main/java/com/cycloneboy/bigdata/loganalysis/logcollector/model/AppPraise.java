package com.cycloneboy.bigdata.loganalysis.logcollector.model;

import lombok.Data;

/** Create by sl on 2020-03-16 21:19 */
@Data
public class AppPraise {

  private int id; // 主键id
  private int userid; // 用户id
  private int target_id; // 点赞的对象id
  private int type; // 点赞类型 1问答点赞 2问答评论点赞 3 文章点赞数4 评论点赞
  private String add_time; // 添加时间
}
