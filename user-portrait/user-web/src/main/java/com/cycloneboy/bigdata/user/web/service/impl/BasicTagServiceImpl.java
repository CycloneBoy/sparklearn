package com.cycloneboy.bigdata.user.web.service.impl;

import com.cycloneboy.bigdata.user.web.domain.TreeMenuNode;
import com.cycloneboy.bigdata.user.web.domain.dto.BasicTagRequest;
import com.cycloneboy.bigdata.user.web.entity.BasicTag;
import com.cycloneboy.bigdata.user.web.repository.BasicTagRepository;
import com.cycloneboy.bigdata.user.web.service.BasicTagService;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

/**
 * Create by  sl on 2021-03-27 18:24
 */
@Slf4j
@Service
public class BasicTagServiceImpl implements BasicTagService {

  @Autowired
  private BasicTagRepository basicTagRepository;

  @Override
  public Page<BasicTag> getAllBasicTag(BasicTagRequest request) {

    PageRequest pageRequest = PageRequest.of(request.getPageNumber(), request.getPageSize(), Sort.by("id"));
    Page<BasicTag> all = basicTagRepository.findAll(pageRequest);
//    List<BasicTag>  =
    return all;
  }

  @Override
  public List<TreeMenuNode> getAllBasicTagTree() {
    List<BasicTag> list = basicTagRepository.findAll();

    List<TreeMenuNode> treeNodeList = new ArrayList<>();
    //转换数据,这个是前端需要的格式。
    list.forEach(t -> {
      TreeMenuNode treeMenuNode = new TreeMenuNode();
      treeMenuNode.setId(t.getId().toString());
      treeMenuNode.setParentId(t.getPid().toString());
      treeMenuNode.setName(t.getName());
      treeMenuNode.setIndustry(t.getIndustry());
      treeMenuNode.setSort(t.getLevel());
      treeNodeList.add(treeMenuNode);
    });

    List<TreeMenuNode> result = treeNodeList.stream()
        // 查找第一级菜单
        .filter(meun -> meun.getParentId().equals("-1"))
        // 查找子菜单并放到第一级菜单中
        .map(menu -> {
          menu.setChildren(getChildren(menu, treeNodeList));
          return menu;
        })
        // 根据排序字段排序
        .sorted(Comparator.comparingInt(menu -> (menu.getSort() == null ? 0 : menu.getSort())))
        // 把处理结果收集成一个 List 集合
        .collect(Collectors.toList());
    return result;
  }


  /**
   * 递归获取子菜单
   *
   * @param root 当前菜单
   * @param all  总的数据
   * @return 子菜单
   */
  private List<TreeMenuNode> getChildren(TreeMenuNode root, List<TreeMenuNode> all) {
    List<TreeMenuNode> children = all.stream()
        // 根据 父菜单 ID 查找当前菜单 ID，以便于找到 当前菜单的子菜单
        .filter(menu -> menu.getParentId().equals(root.getId()))
        // 递归查找子菜单的子菜单
        .map((menu) -> {
          menu.setChildren(getChildren(menu, all));
          return menu;
        })
        // 根据排序字段排序
        .sorted(Comparator.comparingInt(menu -> (menu.getSort() == null ? 0 : menu.getSort())))
        // 把处理结果收集成一个 List 集合
        .collect(Collectors.toList());
    return children;
  }
}
