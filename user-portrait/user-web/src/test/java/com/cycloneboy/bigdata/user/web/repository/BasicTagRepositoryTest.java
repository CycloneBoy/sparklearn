package com.cycloneboy.bigdata.user.web.repository;

import com.cycloneboy.bigdata.user.web.common.BaseCloudTest;
import com.cycloneboy.bigdata.user.web.domain.TreeMenuNode;
import com.cycloneboy.bigdata.user.web.entity.BasicTag;
import com.cycloneboy.bigdata.user.web.utils.TreeUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Create by  sl on 2021-03-27 18:00
 */
@Slf4j
public class BasicTagRepositoryTest extends BaseCloudTest {

  @Autowired
  private BasicTagRepository basicTagRepository;

  @Autowired
  private ModelRepository modelRepository;


  @Test
  public void testSelectAll() {
    log.info("basicTagRepository - total:{}", basicTagRepository.getTotal());

    basicTagRepository.findAllByPid(3).stream().forEach(basicTag -> log.info(basicTag.toString()));

    log.info("modelRepository - total:{}", modelRepository.getTotal());

    modelRepository.findAll()
        .stream().forEach(model -> log.info(model.toString()));

  }


  @Test
  public void testTree() {
    List<BasicTag> allTags = basicTagRepository.findAll();

    List<TreeMenuNode> treeNodeList = new ArrayList<>();
    //转换数据,这个是前端需要的格式。
    allTags.forEach(t -> {
      TreeMenuNode treeMenuNode = new TreeMenuNode();
      treeMenuNode.setId(t.getId().toString());
      treeMenuNode.setParentId(t.getPid().toString());
      treeMenuNode.setName(t.getName());
      treeMenuNode.setIndustry(t.getIndustry());
      treeMenuNode.setSort(t.getLevel());
      treeNodeList.add(treeMenuNode);
    });
    //分组
    Map<String, List<TreeMenuNode>> collect = treeNodeList.stream().collect(Collectors.groupingBy(TreeMenuNode::getParentId));
    //树形结构 肯定有一个根部，我的这个根部的就是parentId.euqal("0"),而且只有一个就get（"0"）
    TreeMenuNode treeMenuNode = collect.get("1").get(0);
    //拼接数据
    List<TreeMenuNode> treeMenuNodes = TreeUtils.forEach(collect, treeMenuNode);

    for (TreeMenuNode menuNode : treeMenuNodes) {
      log.info(menuNode.toString());
    }

  }

}