package com.cycloneboy.bigdata.communication.entity;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Data;

/**
 * Create by sl on 2019-12-07 12:20 <br>
 * 联系人表格
 */
@Data
@Entity
@Table(name = "tb_contacts")
public class ContactInfo {

  /** ID */
  @Id @GeneratedValue private Integer id;

  private String telephone;

  private String name;
}
