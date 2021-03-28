package com.cycloneboy.bigdata.user.web.domain;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.domain.Pageable;

/**
 * create by CycloneBoy on 2019-06-22 21:53
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageResponse {

  private Long totalItem;

  private Integer totalPages;

  private Integer pageNumber;

  private Integer pageSize;

  private Integer numberOfElements;

  private Object data;

  public PageResponse(List<?> result) {
    this.data = result;
    this.totalItem = (long) result.size();
  }

  public PageResponse(List<?> result, long totalItem) {
    this.data = result;
    this.totalItem = totalItem;
  }

  public PageResponse(Object data, Long totalItem, Integer totalPages, Integer pageNumber, Integer pageSize, Integer numberOfElements) {
    this.data = data;
    this.totalItem = totalItem;
    this.totalPages = totalPages;
    this.pageNumber = pageNumber;
    this.pageSize = pageSize;
    this.numberOfElements = numberOfElements;
  }

  public PageResponse(List<?> result, Long totalItem, Integer totalPages, Pageable pageable) {
    this.data = result;
    this.totalItem = totalItem;
    this.totalPages = totalPages;
    this.pageNumber = pageable.getPageNumber();
    this.pageSize = pageable.getPageSize();
    this.numberOfElements = result.size();
  }
}
