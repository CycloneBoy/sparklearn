package com.cycloneboy.bigdata.communication.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Create by sl on 2019-12-07 14:53 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CallLogRequest {

  private String telephone;

  private String name;

  private Integer year;

  private Integer month;

  private Integer day;
}
