package com.cycloneboy.bigdata.communication.repository;

import com.cycloneboy.bigdata.communication.entity.CallInfo;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

/** Create by sl on 2019-11-30 13:41 */
@Repository
public interface CallInfoRepository extends JpaRepository<CallInfo, String> {

  @Query(value = "select count(*) from tb_call", nativeQuery = true)
  Long getTotal();

  List<CallInfo> findAllByIdContactAndIdDateDimension(Integer contactId, Integer dateDimensionId);

  List<CallInfo> findAllByIdContactAndIdDateDimensionIn(
      Integer contactId, List<Integer> dateDimensionIds);
}
