package com.cycloneboy.bigdata.communication.model;

import com.cycloneboy.bigdata.communication.annocation.Column;
import com.cycloneboy.bigdata.communication.annocation.RowKey;
import com.cycloneboy.bigdata.communication.annocation.TableRef;
import com.cycloneboy.bigdata.communication.common.Constants;
import com.cycloneboy.bigdata.communication.conf.ConfigurationManager;
import com.cycloneboy.bigdata.communication.utils.DateUtils;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/** Create by sl on 2019-12-04 13:21 */
@NoArgsConstructor
@Data
@TableRef("call:calllog")
public class CallLog {

  @RowKey private String rowkey;

  /** 主叫号码 */
  @Column(family = "caller")
  private String caller;

  /** 被叫号码 */
  @Column(family = "caller")
  private String callee;

  /** 建立通话的时间 */
  @Column(family = "caller")
  private String buildTime;

  /** 建立通话的时间戳 */
  @Column(family = "caller")
  private Long buildTimeTs;

  /** 通话总时长 */
  @Column(family = "caller")
  private String duration;

  /** 是主叫还是被叫 */
  @Column(family = "caller")
  private String flag;

  /**
   * 形式：15837312345,13737312345,2017-01-09 08:09:10,0360<br>
   * 解析calllog
   *
   * @param line msg: callerPhone,calleePhone,callTime,duration
   */
  public CallLog(String line) {
    String[] split = line.split(Constants.DELIMITER_COMMA());

    caller = split[0];
    callee = split[1];
    buildTime = DateUtils.convertDateTime(split[2]);
    buildTimeTs = DateUtils.parseTime(split[2]).getTime();
    duration = split[3];

    String callerFlag =
        ConfigurationManager.config().getString(Constants.CONF_HBASE_CALLLOG_CALLER_FLAG());
    flag = StringUtils.isEmpty(callerFlag) ? "1" : callerFlag;
  }

  /**
   * 生成rowkey <br>
   * regionCode_call1_buildTime_call2_flag_duration
   *
   * @param regionCode 分区编号
   * @param callLog 通话话单
   * @return 生成rowkey
   */
  public static String genRowKey(String regionCode, CallLog callLog) {
    StringBuilder sb = new StringBuilder();
    sb.append(regionCode)
        .append(Constants.DELIMITER_UNDERLINE())
        .append(callLog.getCaller())
        .append(Constants.DELIMITER_UNDERLINE())
        .append(callLog.getBuildTime())
        .append(Constants.DELIMITER_UNDERLINE())
        .append(callLog.getCallee())
        .append(Constants.DELIMITER_UNDERLINE())
        .append(callLog.getFlag())
        .append(Constants.DELIMITER_UNDERLINE())
        .append(callLog.getDuration());

    return sb.toString();
  }
}
