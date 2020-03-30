package com.cycloneboy.bigdata.loganalysis.logcollector.model;

import lombok.Data;

/** Create by sl on 2020-03-16 21:15 错误日志Bean */
@Data
public class AppErrorLog {

  private String errorBrief; // 错误摘要
  private String errorDetail; // 错误详情
}
