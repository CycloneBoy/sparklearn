package com.cycloneboy.bigdata.communication.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Data;

/**
 * Create by sl on 2019-11-30 13:25 <br>
 * 通话话单表格
 */
@Data
@Entity
@Table(name = "tb_call")
public class CallInfo {

  /** ID */
  @Id
  @Column(name = "id_date_contact")
  private String id;

  private Integer idDateDimension;

  private Integer idContact;

  private Integer callSum;

  private Integer callDurationSum;
}
