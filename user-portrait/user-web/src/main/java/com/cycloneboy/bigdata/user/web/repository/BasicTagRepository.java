package com.cycloneboy.bigdata.user.web.repository;

import com.cycloneboy.bigdata.user.web.entity.BasicTag;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

/**
 * Create by sl on 2019-11-30 13:41
 */
@Repository
public interface BasicTagRepository extends JpaRepository<BasicTag, Long> {

  @Query(value = "select count(*) from tbl_basic_tag", nativeQuery = true)
  Long getTotal();

  List<BasicTag> findAllByPid(Integer pid);

}
