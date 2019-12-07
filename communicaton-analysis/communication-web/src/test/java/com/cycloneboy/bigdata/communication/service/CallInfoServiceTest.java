package com.cycloneboy.bigdata.communication.service;

import com.cycloneboy.bigdata.communication.common.BaseCloudTest;
import com.cycloneboy.bigdata.communication.domain.CallLog;
import com.cycloneboy.bigdata.communication.domain.CallLogRequest;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Create by sl on 2019-12-07 15:30 */
@Slf4j
public class CallInfoServiceTest extends BaseCloudTest {

  @Autowired private CallInfoService callInfoService;

  @Test
  public void getCallLogByDay() {
    log.info("-------------by day--------------");
    CallLogRequest request = new CallLogRequest("15542823911", null, 2019, 10, 1);
    List<CallLog> callLogList = callInfoService.getCallLogByDay(request);
    callLogList.forEach(callLog -> log.info(callLog.toString()));

    log.info("--------------by month-------------");

    request = new CallLogRequest("15542823911", null, 2019, 10, -1);
    callLogList = callInfoService.getCallLogByDay(request);
    callLogList.forEach(callLog -> log.info(callLog.toString()));

    log.info("--------------by year-------------");
    request = new CallLogRequest("15542823911", null, 2019, -1, -1);
    callLogList = callInfoService.getCallLogByDay(request);
    callLogList.forEach(callLog -> log.info(callLog.toString()));
  }

  @Test
  public void testGetByMonth() {
    log.info("-------------by month--------------");
    CallLogRequest request = new CallLogRequest("15542823911", null, 2019, 10, 0);
    List<CallLog> callLogList = callInfoService.getCallLogByMonth(request);
    log.info("size: {}", callLogList.size());
    callLogList.forEach(callLog -> log.info(callLog.toString()));
  }

  @Test
  public void testGetCallLogByYear() {
    log.info("-------------by year-------------");
    CallLogRequest request = new CallLogRequest("15542823911", null, 2019, 0, 0);
    List<CallLog> callLogList = callInfoService.getCallLogByYear(request);
    log.info("size: {}", callLogList.size());
    callLogList.forEach(callLog -> log.info(callLog.toString()));
  }
}
