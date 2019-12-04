package com.cycloneboy.bigdata.communication.producer;

import com.cycloneboy.bigdata.communication.common.Constants;
import com.cycloneboy.bigdata.communication.conf.ConfigurationManagerUtils;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Create by sl on 2019-12-03 16:08 */
@Slf4j
public class LogProducer {

  private String startTime = "2019-01-01";
  private String endTime = "2019-12-01";

  public Integer sizeOfSeconds = 2;
  private Integer durationMax = 3600;
  public String filePath;

  private List<String> phoneList = new ArrayList<>();
  private Map<String, String> phoneNameMap = new HashMap<>();

  public void initPhone() {
    phoneNameMap.put("17078388295", "李雁");
    phoneNameMap.put("13980337439", "卫艺");
    phoneNameMap.put("14575535933", "仰莉");
    phoneNameMap.put("19902496992", "陶欣悦");
    phoneNameMap.put("18549641558", "施梅梅");
    phoneNameMap.put("17005930322", "金虹霖");
    phoneNameMap.put("18468618874", "魏明艳");
    phoneNameMap.put("18576581848", "华贞");
    phoneNameMap.put("15978226424", "华啟倩");
    phoneNameMap.put("15542823911", "仲采绿");
    phoneNameMap.put("17526304161", "卫丹");
    phoneNameMap.put("15422018558", "戚丽红");
    phoneNameMap.put("17269452013", "何翠柔");
    phoneNameMap.put("17764278604", "钱溶艳");
    phoneNameMap.put("15711910344", "钱琳");
    phoneNameMap.put("15714728273", "缪静欣");
    phoneNameMap.put("16061028454", "焦秋菊");
    phoneNameMap.put("16264433631", "吕访琴");
    phoneNameMap.put("17601615878", "沈丹");
    phoneNameMap.put("15897468949", "褚美丽");

    phoneList.addAll(phoneNameMap.keySet());

    //    startTime = ConfigurationManagerUtils.config()
    startTime = ConfigurationManagerUtils.config.getString(Constants.CONF_START_DATE());
    endTime = ConfigurationManagerUtils.config.getString(Constants.CONF_END_DATE());
    sizeOfSeconds =
        ConfigurationManagerUtils.config.getInt(Constants.CONF_LOG_MOCK_SIZE_PER_SECONDS());
    if (sizeOfSeconds < 0) sizeOfSeconds = 1;
    durationMax =
        ConfigurationManagerUtils.config.getInt(Constants.CONF_LOG_DURAION_MAX_DURATION());
    if (durationMax < 0) durationMax = 3600;

    filePath = ConfigurationManagerUtils.config.getString(Constants.CONF_LOG_OUT_DIR());
  }

  /** 形式：15837312345,13737312345,2017-01-09 08:09:10,0360 */
  public String product() throws ParseException {
    int callerIndex = (int) (Math.random() * phoneList.size());
    String caller = phoneList.get(callerIndex);
    String callerName = phoneNameMap.get(caller);

    String callee = null;
    String calleeName = null;

    while (true) {
      int calleeIndex = (int) (Math.random() * phoneList.size());
      callee = phoneList.get(calleeIndex);
      calleeName = phoneNameMap.get(callee);
      if (!callee.equals(caller)) break;
    }

    String buildTime = randomBuildTime(startTime, endTime);

    DecimalFormat decimalFormat = new DecimalFormat("0000");

    String duration = decimalFormat.format((int) (durationMax * Math.random()));

    StringBuilder sb = new StringBuilder();
    sb.append(caller)
        .append(Constants.DELIMITER_COMMA())
        .append(callee)
        .append(Constants.DELIMITER_COMMA())
        .append(buildTime)
        .append(Constants.DELIMITER_COMMA())
        .append(duration);
    return sb.toString();
  }

  /**
   * 根据传入的时间区间，在此范围内随机通话建立的时间<br>
   * startTimeTS + (endTimeTs - startTimeTs) * Math.random();
   *
   * @param startTime
   * @param endTime
   * @return
   */
  public String randomBuildTime(String startTime, String endTime) throws ParseException {
    SimpleDateFormat sdf1 = new SimpleDateFormat(Constants.DATETIME_FORMATE_YYYYMMDD());

    Date startDate = sdf1.parse(startTime);
    Date endDate = sdf1.parse(endTime);

    if (endDate.before(startDate)) return null;

    long randomTs =
        startDate.getTime() + (long) ((endDate.getTime() - startDate.getTime()) * Math.random());

    Date resultDate = new Date(randomTs);
    return new SimpleDateFormat(Constants.DATETIME_FORMATE_STANDARD()).format(resultDate);
  }

  /**
   * 写入log文件
   *
   * @param filePath
   */
  public void writeLog(String filePath, int perSecondSize) {
    if (perSecondSize <= 0) return;
    try {
      OutputStreamWriter outputStreamWriter =
          new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8");

      while (true) {
        String result = product();
        log.info(result);
        outputStreamWriter.write(result + "\r\n");
        // 一定要手动flush才可以确保每条数据都写入到文件一次
        outputStreamWriter.flush();
        try {
          Thread.sleep(1000 / perSecondSize);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (ParseException | IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    LogProducer logProducer = new LogProducer();

    String filePath;
    Integer perSecondSize;

    if (args == null || args.length <= 0) {
      filePath = logProducer.filePath;
      perSecondSize = logProducer.sizeOfSeconds;
      log.info("args is empty,so use filePath :{}", filePath);
    } else if (args.length == 1) {
      filePath = args[0];
      perSecondSize = logProducer.sizeOfSeconds;
    } else {
      filePath = args[0];
      perSecondSize = Integer.parseInt(args[1]);
    }

    logProducer.initPhone();
    logProducer.writeLog(filePath, perSecondSize);
  }
}
