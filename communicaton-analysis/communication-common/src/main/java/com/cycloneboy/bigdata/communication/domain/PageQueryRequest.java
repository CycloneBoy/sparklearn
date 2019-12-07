package com.cycloneboy.bigdata.communication.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Create by sl on 2019-08-11 13:28 */
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PageQueryRequest {

  @JsonProperty("page_number")
  private Integer pageNumber = 1;

  @JsonProperty("page_size")
  private Integer pageSize = 10;

  public Integer getPageNumber() {
    if (pageNumber < 0) {
      pageNumber = 1;
    }
    return pageNumber;
  }

  public Integer getPageSize() {
    if (pageSize < 0) {
      pageSize = 10;
    }
    return pageSize;
  }
}
