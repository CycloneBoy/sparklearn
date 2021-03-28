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
@Table(name = "tbl_model")
public class Model {

  /**
   * ID
   */
  @Id
  @Column(name = "id")
  private Long id;

  private Long tagId;

  private Integer type;

  private String modelName;

  private String modelMain;

  private String modelPath;

  private String scheTime;

  private Date ctime;

  private Date utime;

  private Integer state;

  private String remark;

  private String operator;

  private String operation;

  private String args;

}
