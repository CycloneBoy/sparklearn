package com.cycloneboy.bigdata.user.web.entity;

import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Data;

/**
 * Create by sl on 2019-11-30 13:25 <br> 基础标签表
 */
@Data
@Entity
@Table(name = "tbl_basic_tag")
public class BasicTag {

  /**
   * ID
   */
  @Id
  @Column(name = "id")
  private Integer id;

  private String name;

  private String industry;

  private String rule;

  private String business;

  private Integer level;

  private Integer pid;

  private Date ctime;

  private Date utime;

  private Integer state;

  private String remark;
}
