package com.cycloneboy.bigdata.user.web.domain.dto;

import com.cycloneboy.bigdata.user.web.domain.PageQueryRequest;
import lombok.Data;

/**
 * Create by sl on 2019-08-11 13:33
 */
@Data
public class BasicTagRequest extends PageQueryRequest {

  private Long id;

  private String name;

  private Integer level;

  private Integer pid;

}
