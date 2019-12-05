package com.cycloneboy.bigdata.communication.utils;

import java.util.List;

/**
 * Create by sl on 2019-12-05 11:48 <br>
 * 该类主要用于根据用户传入的手机号以及开始和结束时间点，按月生成多组rowkey
 */
public class ScanRowkeyUtil {

  private String telephone;
  private String startDateString;
  private String stopDateString;
  List<String[]> list = null;

  int index = 0;
}
