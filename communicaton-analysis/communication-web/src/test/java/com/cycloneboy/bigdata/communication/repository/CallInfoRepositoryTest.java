package com.cycloneboy.bigdata.communication.repository;

import com.cycloneboy.bigdata.communication.common.BaseCloudTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Create by sl on 2019-12-07 13:01 */
@Slf4j
public class CallInfoRepositoryTest extends BaseCloudTest {

  @Autowired private CallInfoRepository callInfoRepository;

  @Autowired private CallIntimacyRepository callIntimacyRepository;

  @Autowired private ContactInfoRepository contactInfoRepository;

  @Autowired private DimensionDateRepository dimensionDateRepository;

  @Test
  public void getTotal() {

    log.info("callInfoRepository - total:{}", callInfoRepository.getTotal());
    log.info("callIntimacyRepository - total:{}", callIntimacyRepository.getTotal());
    log.info("contactInfoRepository - total:{}", contactInfoRepository.getTotal());
    log.info("dimensionDateRepository - total:{}", dimensionDateRepository.getTotal());
  }
}
