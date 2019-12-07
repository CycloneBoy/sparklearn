package com.cycloneboy.bigdata.communication.repository;

import com.cycloneboy.bigdata.communication.entity.DimensionDate;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

/** Create by sl on 2019-12-07 13:00 */
@Repository
public interface DimensionDateRepository extends JpaRepository<DimensionDate, Integer> {

  @Query(value = "select count(*) from tb_dimension_date", nativeQuery = true)
  Long getTotal();

  DimensionDate findAllByYearAndMonthAndDay(Integer year, Integer month, Integer day);

  List<DimensionDate> findAllByYearAndMonth(Integer year, Integer month);

  List<DimensionDate> findAllByYear(Integer year);

  List<DimensionDate> findAllByYearAndMonthInAndDay(Integer year, List<Integer> month, Integer day);
}
