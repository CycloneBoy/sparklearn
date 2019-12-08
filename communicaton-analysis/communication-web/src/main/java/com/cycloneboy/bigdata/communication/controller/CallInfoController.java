package com.cycloneboy.bigdata.communication.controller;

import com.cycloneboy.bigdata.communication.domain.BaseResponse;
import com.cycloneboy.bigdata.communication.domain.CallLog;
import com.cycloneboy.bigdata.communication.domain.CallLogRequest;
import com.cycloneboy.bigdata.communication.domain.PageResponse;
import com.cycloneboy.bigdata.communication.service.CallInfoService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Create by sl on 2019-12-07 18:40 */
@Slf4j
@RestController
@RequestMapping("/api/call")
public class CallInfoController {

  @Autowired private CallInfoService callInfoService;

  @GetMapping("/byday")
  public BaseResponse queryCallLogByDay(CallLogRequest request) {

    List<CallLog> callLogList = callInfoService.getCallLogByDay(request);
    return new PageResponse(callLogList);
  }

  @GetMapping("/bymonth")
  public BaseResponse queryCallLogByMonth(CallLogRequest request) {

    List<CallLog> callLogList = callInfoService.getCallLogByMonth(request);
    return new PageResponse(callLogList);
  }

  @GetMapping("/byyear")
  public BaseResponse queryCallLogByYear(CallLogRequest request) {

    List<CallLog> callLogList = callInfoService.getCallLogByYear(request);
    return new PageResponse(callLogList);
  }
}
