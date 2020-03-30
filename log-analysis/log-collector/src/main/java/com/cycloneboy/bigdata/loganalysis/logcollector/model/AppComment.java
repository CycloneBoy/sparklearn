package com.cycloneboy.bigdata.loganalysis.logcollector.model;

import lombok.Data;

/** Create by sl on 2020-03-16 21:18 */
@Data
public class AppComment {

  private int comment_id; // 评论表
  private int userid; // 用户id
  private int p_comment_id; // 父级评论id(为0则是一级评论,不为0则是回复)
  private String content; // 评论内容
  private String addtime; // 创建时间
  private int other_id; // 评论的相关id
  private int praise_count; // 点赞数量
  private int reply_count; // 回复数量
}
