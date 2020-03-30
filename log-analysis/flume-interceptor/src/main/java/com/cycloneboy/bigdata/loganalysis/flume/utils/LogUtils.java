package com.cycloneboy.bigdata.loganalysis.flume.utils;

import org.apache.commons.lang.math.NumberUtils;

/** Create by sl on 2020-03-16 01:50 */
public class LogUtils {

  public static boolean validateStart(String log) {
    // {"action":"1","ar":"MX","ba":"HTC","detail":"542","en":"start","entry":"2","extend1":"","g":"S3HQ7LKM@gmail.com","hw":"640*960","l":"en","la":"-43.4","ln":"-98.3","loading_time":"10","md":"HTC-5","mid":"993","nw":"WIFI","open_ad_type":"1","os":"8.2.1","sr":"D","sv":"V2.9.0","t":"1559551922019","uid":"993","vc":"0","vn":"1.1.5"}

    if (log == null) {
      return false;
    }

    // 校验json
    return log.trim().startsWith("{") && log.trim().endsWith("}");
  }

  public static boolean validateEvent(String log) {
    // 服务器时间 | json
    // 1549696569054 |
    // {"cm":{"ln":"-89.2","sv":"V2.0.4","os":"8.2.0","g":"M67B4QYU@gmail.com","nw":"4G","l":"en","vc":"18","hw":"1080*1920","ar":"MX","uid":"u8678","t":"1549679122062","la":"-27.4","md":"sumsung-12","vn":"1.1.3","ba":"Sumsung","sr":"Y"},"ap":"weather","et":[]}

    // 1 切割
    String[] logContents = log.split("\\|");

    // 2 校验
    if (logContents.length != 2) {
      return false;
    }

    // 3 校验服务器时间
    if (logContents[0].length() != 13 || !NumberUtils.isDigits(logContents[0])) {
      return false;
    }

    // 4 校验json
    return logContents[1].trim().startsWith("{") && logContents[1].trim().endsWith("}");
  }
}
