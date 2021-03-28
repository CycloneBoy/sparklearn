package com.cycloneboy.bigdata.user.web.service;

import com.alibaba.fastjson.JSON;
import com.cycloneboy.bigdata.user.web.common.BaseCloudTest;
import com.cycloneboy.bigdata.user.web.domain.TreeMenuNode;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Create by  sl on 2021-03-28 09:18
 */
@Slf4j
public class BasicTagServiceImplTest extends BaseCloudTest {

  @Autowired
  private BasicTagService basicTagService;

  @Test
  public void getAllBasicTagTree() {
    List<TreeMenuNode> allBasicTagTree = basicTagService.getAllBasicTagTree();

    String json = JSON.toJSONString(allBasicTagTree);

    log.info(json);
  }
}