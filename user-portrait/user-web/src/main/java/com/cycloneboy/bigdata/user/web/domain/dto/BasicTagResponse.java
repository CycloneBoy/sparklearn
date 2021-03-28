package com.cycloneboy.bigdata.user.web.domain.dto;

import com.cycloneboy.bigdata.user.web.entity.BasicTag;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.BeanUtils;

/**
 * Create by sl on 2019-11-30 13:25 <br> 基础标签表
 */
@NoArgsConstructor
@Data
public class BasicTagResponse {

  private Integer id;

  private String name;

  private String industry;

  private String rule;

  private String business;

  private Integer level;

  private Integer pid;


  public static BasicTagResponse of(BasicTag basicTag) {
    BasicTagResponse basicTagResponse = new BasicTagResponse();
    BeanUtils.copyProperties(basicTag, basicTagResponse);

    return basicTagResponse;
  }
}
