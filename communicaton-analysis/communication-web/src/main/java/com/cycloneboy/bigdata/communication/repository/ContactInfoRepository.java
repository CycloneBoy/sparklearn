package com.cycloneboy.bigdata.communication.repository;

import com.cycloneboy.bigdata.communication.entity.ContactInfo;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

/** Create by sl on 2019-11-30 13:41 */
@Repository
public interface ContactInfoRepository extends JpaRepository<ContactInfo, Integer> {

  @Query(value = "select count(*) from tb_contacts", nativeQuery = true)
  Long getTotal();

  List<ContactInfo> findAllByTelephoneLike(String telephone);

  List<ContactInfo> findAllByNameLike(String name);

  ContactInfo findByTelephone(String telephone);

  ContactInfo findByName(String name);
}
