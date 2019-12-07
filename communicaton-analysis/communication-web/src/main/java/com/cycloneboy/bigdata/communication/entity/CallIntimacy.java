package com.cycloneboy.bigdata.communication.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Data;

/**
 * Create by sl on 2019-12-07 12:20 <br>
 * 通话亲密度表格
 */
@Data
@Entity
@Table(name = "tb_intimacy")
public class CallIntimacy {

  /** ID */
  @Id @GeneratedValue private Integer id;

  /** 排名 */
  private Integer intimacyRank;

  /** 主叫人号码ID */
  @Column(name = "contact_id1")
  private Integer contactCallerId;

  /** 被叫人号码ID */
  @Column(name = "contact_id2")
  private Integer contactCalleeId;

  /** 通话次数 */
  private Integer callCount;

  /** 通话时间 */
  private Integer callDurationCount;
}
