package com.cycloneboy.bigdata.user.web.domain;

import com.alibaba.fastjson.annotation.JSONField;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Create by  sl on 2021-03-28 08:46
 */

@AllArgsConstructor
@NoArgsConstructor
public class TreeMenuNode {

  @JSONField(name = "key")
  private String id;

  @JSONField(name = "title")
  private String name;

  private String industry;

  private String parentId;

  private Integer sort;

  private List<TreeMenuNode> children;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getIndustry() {
    return industry;
  }

  public void setIndustry(String industry) {
    this.industry = industry;
  }

  public String getParentId() {
    return parentId;
  }

  public void setParentId(String parentId) {
    this.parentId = parentId;
  }

  public Integer getSort() {
    return sort;
  }

  public void setSort(Integer sort) {
    this.sort = sort;
  }

  public List<TreeMenuNode> getChildren() {
    return children;
  }

  public void setChildren(List<TreeMenuNode> children) {
    this.children = children;
  }
}
