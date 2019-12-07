package com.cycloneboy.bigdata.communication.domain;

import lombok.Data;

/** Create by sl on 2019-12-07 14:53 */
@Data
public class CallLog {

  private String telephone;

  private String name;

  private Integer year;

  private Integer month;

  private Integer day;

  private Integer callSum;

  private Integer callDurationSum;
}
