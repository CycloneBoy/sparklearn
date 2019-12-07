package com.cycloneboy.bigdata.communication.repository;

import com.cycloneboy.bigdata.communication.entity.CallIntimacy;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

/** Create by sl on 2019-11-30 13:41 */
@Repository
public interface CallIntimacyRepository extends JpaRepository<CallIntimacy, Integer> {

  @Query(value = "select count(*) from tb_intimacy", nativeQuery = true)
  Long getTotal();
}
