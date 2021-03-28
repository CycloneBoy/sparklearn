package com.cycloneboy.bigdata.user.web.repository;

import com.cycloneboy.bigdata.user.web.entity.Model;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

/**
 * Create by sl on 2019-11-30 13:41
 */
@Repository
public interface ModelRepository extends JpaRepository<Model, Long> {

  @Query(value = "select count(*) from tbl_model", nativeQuery = true)
  Long getTotal();


}
