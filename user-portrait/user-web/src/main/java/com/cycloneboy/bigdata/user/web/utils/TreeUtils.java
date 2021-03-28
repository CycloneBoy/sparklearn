package com.cycloneboy.bigdata.user.web.utils;

import com.cycloneboy.bigdata.user.web.domain.TreeMenuNode;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Create by  sl on 2021-03-28 08:48
 */
@Slf4j
public class TreeUtils {

  public static List<TreeMenuNode> forEach(Map<String, List<TreeMenuNode>> collect, TreeMenuNode treeMenuNode) {
    List<TreeMenuNode> treeMenuNodes = collect.get(treeMenuNode.getId());
    if (collect.get(treeMenuNode.getId()) != null) {
      //排序
      treeMenuNodes.sort((u1, u2) -> u1.getSort().compareTo(u2.getSort()));
      treeMenuNodes.stream().sorted(Comparator.comparing(TreeMenuNode::getSort)).collect(Collectors.toList());
      treeMenuNode.setChildren(treeMenuNodes);
      treeMenuNode.getChildren().forEach(t -> {
        forEach(collect, t);
      });
    }

    return treeMenuNodes;
  }


}
