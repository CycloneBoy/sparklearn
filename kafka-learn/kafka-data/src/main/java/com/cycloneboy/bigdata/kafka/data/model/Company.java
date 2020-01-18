package com.cycloneboy.bigdata.kafka.data.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Create by sl on 2020-01-18 15:14 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Company {

  private String name;

  private String address;
}
