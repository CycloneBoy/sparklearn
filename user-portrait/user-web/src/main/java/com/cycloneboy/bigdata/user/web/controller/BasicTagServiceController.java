package com.cycloneboy.bigdata.user.web.controller;

import com.cycloneboy.bigdata.user.web.config.annotation.ResponseAnnotation;
import com.cycloneboy.bigdata.user.web.domain.PageResponse;
import com.cycloneboy.bigdata.user.web.domain.TreeMenuNode;
import com.cycloneboy.bigdata.user.web.domain.dto.BasicTagRequest;
import com.cycloneboy.bigdata.user.web.domain.dto.BasicTagResponse;
import com.cycloneboy.bigdata.user.web.entity.BasicTag;
import com.cycloneboy.bigdata.user.web.service.BasicTagService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Create by  sl on 2021-03-28 09:47
 */
@Api(tags = "标签管理")
@ResponseAnnotation
@RestController
@RequestMapping("/api/tag")
public class BasicTagServiceController {

  @Autowired
  private BasicTagService basicTagService;

  @ApiOperation(value = "以树型结果获取全部标签")
  @GetMapping("/tree")
  public List<TreeMenuNode> getAllTags() {
    return basicTagService.getAllBasicTagTree();
  }

  @ApiOperation(value = "分页查询标签")
  @GetMapping("/list")
  public PageResponse getAllTags(BasicTagRequest request) {

    Page<BasicTag> allBasicTag = basicTagService.getAllBasicTag(request);

    List<BasicTag> content = allBasicTag.getContent();
    List<BasicTagResponse> basicTagResponses = new ArrayList<>();

    for (BasicTag basicTag : content) {
      basicTagResponses.add(BasicTagResponse.of(basicTag));
    }

    PageResponse response = new PageResponse(basicTagResponses, allBasicTag.getTotalElements(), allBasicTag.getTotalPages(),
        allBasicTag.getPageable());
    return response;
  }

}
