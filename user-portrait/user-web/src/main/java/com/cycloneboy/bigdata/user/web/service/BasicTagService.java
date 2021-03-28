package com.cycloneboy.bigdata.user.web.service;

import com.cycloneboy.bigdata.user.web.domain.TreeMenuNode;
import com.cycloneboy.bigdata.user.web.domain.dto.BasicTagRequest;
import com.cycloneboy.bigdata.user.web.entity.BasicTag;
import java.util.List;
import org.springframework.data.domain.Page;

/**
 * Create by  sl on 2021-03-28 09:16
 */
public interface BasicTagService {

  Page<BasicTag> getAllBasicTag(BasicTagRequest request);

  List<TreeMenuNode> getAllBasicTagTree();

}
