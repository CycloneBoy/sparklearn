package com.cycloneboy.bigdata.communication.domain;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** create by CycloneBoy on 2019-06-22 21:53 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageResponse extends BaseResponse {

  private long totalItem;

  public PageResponse(List<?> result) {
    super(result);
    this.totalItem = result.size();
  }

  public PageResponse(List<?> result, long totalItem) {
    super(result);
    this.totalItem = totalItem;
  }
}
