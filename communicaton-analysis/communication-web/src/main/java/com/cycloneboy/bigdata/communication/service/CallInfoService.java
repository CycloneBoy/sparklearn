package com.cycloneboy.bigdata.communication.service;

import com.cycloneboy.bigdata.communication.common.Constants;
import com.cycloneboy.bigdata.communication.domain.CallLog;
import com.cycloneboy.bigdata.communication.domain.CallLogRequest;
import com.cycloneboy.bigdata.communication.entity.CallInfo;
import com.cycloneboy.bigdata.communication.entity.ContactInfo;
import com.cycloneboy.bigdata.communication.entity.DimensionDate;
import com.cycloneboy.bigdata.communication.repository.CallInfoRepository;
import com.cycloneboy.bigdata.communication.repository.ContactInfoRepository;
import com.cycloneboy.bigdata.communication.repository.DimensionDateRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/** Create by sl on 2019-12-07 14:39 */
@Slf4j
@Service
public class CallInfoService {

  @Autowired private CallInfoRepository callInfoRepository;

  @Autowired private ContactInfoRepository contactInfoRepository;

  @Autowired private DimensionDateRepository dimensionDateRepository;

  private static final List<Integer> MONTH_LIST =
      Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
  //  -- 按照年统计：统计某个用户，1年12个月的所有的数据（不精确到day）

  /**
   * 根据用户电话号码和用户查询的年月日查询通话时间次数
   *
   * @param request
   * @return
   */
  public List<CallLog> getCallLogByDay(CallLogRequest request) {
    ContactInfo contact = contactInfoRepository.findByTelephone(request.getTelephone());
    if (contact == null) return new ArrayList<>();

    DimensionDate queryDimension =
        dimensionDateRepository.findAllByYearAndMonthAndDay(
            request.getYear(), request.getMonth(), request.getDay());

    List<CallInfo> callInfoList =
        callInfoRepository.findAllByIdContactAndIdDateDimension(
            contact.getId(), queryDimension.getId());

    log.info("callInfoList : {}", callInfoList.get(0));
    return convetCallInfo(request, contact, callInfoList);
  }

  /**
   * 按月查询通话记录,返回一个月的通话记录
   *
   * @param request
   * @return 一个月的通话记录
   */
  public List<CallLog> getCallLogByMonth(CallLogRequest request) {
    ContactInfo contact = contactInfoRepository.findByTelephone(request.getTelephone());
    if (contact == null) return new ArrayList<>();
    // 查询一年中的某个个月的ID
    List<DimensionDate> dimensionList =
        dimensionDateRepository.findAllByYearAndMonth(request.getYear(), request.getMonth());

    // 过滤按年汇总和按月汇总的数据
    List<DimensionDate> filterDimensionList =
        dimensionList.stream()
            .filter(
                dimensionDate -> dimensionDate.getDay() != Constants.DIMENSION_DATE_DEFAULT_DAY())
            .collect(Collectors.toList());

    return processCallLog(contact, filterDimensionList);
  }

  /**
   * 查询一年的通话记录,按月汇总
   *
   * @param request
   * @return 一年的通话记录,按月汇总
   */
  public List<CallLog> getCallLogByYear(CallLogRequest request) {

    ContactInfo contact = contactInfoRepository.findByTelephone(request.getTelephone());
    if (contact == null) return new ArrayList<>();
    // 查询一年中的12个月的ID
    List<DimensionDate> dimensionList =
        dimensionDateRepository.findAllByYearAndMonthInAndDay(
            request.getYear(), MONTH_LIST, Constants.DIMENSION_DATE_DEFAULT_DAY());

    return processCallLog(contact, dimensionList);
  }

  /**
   * 处理callLog <br>
   * 没有处理当个月或者当年数据为空的情况
   *
   * @param contact 联系人信息
   * @param dimensionList 日历信息
   * @return 符合日历信息的通话记录
   */
  private List<CallLog> processCallLog(ContactInfo contact, List<DimensionDate> dimensionList) {
    List<Integer> diemansionIdList =
        dimensionList.stream().map(DimensionDate::getId).collect(Collectors.toList());
    // 查询符合条件的话单
    List<CallInfo> callInfoList =
        callInfoRepository.findAllByIdContactAndIdDateDimensionIn(
            contact.getId(), diemansionIdList);

    List<CallLog> callLogList = convetCallInfo(contact, callInfoList, dimensionList);
    // 按日期排序
    return callLogList.stream()
        .sorted(Comparator.comparing(CallLog::getDay))
        .collect(Collectors.toList());
  }

  /**
   * 转换 CallInfo to CallLog
   *
   * @param request
   * @param contact
   * @param callInfoList
   * @return
   */
  private List<CallLog> convetCallInfo(
      CallLogRequest request, ContactInfo contact, List<CallInfo> callInfoList) {
    List<CallLog> callLogList = new ArrayList<>();
    callInfoList.forEach(
        callInfo -> {
          CallLog callLog = new CallLog();
          BeanUtils.copyProperties(request, callLog);
          callLog.setName(contact.getName());
          callLog.setCallSum(callInfo.getCallSum());
          callLog.setCallDurationSum(callInfo.getCallDurationSum());
          callLogList.add(callLog);
        });

    return callLogList;
  }

  /**
   * 转换 CallInfo to CallLog
   *
   * @param contact
   * @param callInfoList
   * @param dimensionDateList
   * @return
   */
  private List<CallLog> convetCallInfo(
      ContactInfo contact, List<CallInfo> callInfoList, List<DimensionDate> dimensionDateList) {
    List<CallLog> callLogList = new ArrayList<>();
    callInfoList.forEach(
        callInfo -> {
          CallLog callLog = new CallLog();
          DimensionDate targetDimensionDate =
              dimensionDateList.stream()
                  .filter(
                      dimensionDate -> dimensionDate.getId().equals(callInfo.getIdDateDimension()))
                  .collect(Collectors.toList())
                  .get(0);
          BeanUtils.copyProperties(targetDimensionDate, callLog);

          callLog.setTelephone(contact.getTelephone());
          callLog.setName(contact.getName());

          callLog.setCallSum(callInfo.getCallSum());
          callLog.setCallDurationSum(callInfo.getCallDurationSum());

          callLogList.add(callLog);
        });

    return callLogList;
  }
}
