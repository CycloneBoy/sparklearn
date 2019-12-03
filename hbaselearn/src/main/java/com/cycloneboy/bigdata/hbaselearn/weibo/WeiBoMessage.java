package com.cycloneboy.bigdata.hbaselearn.weibo;

import static com.cycloneboy.bigdata.hbaselearn.common.Constants.TIME_FORMAT;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/** Create by sl on 2019-12-03 12:20 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WeiBoMessage {

  private String uid;
  private Long timestamp;
  private String content;

  public String getDate() {
    if (timestamp == null) {
      return "";
    }
    return new DateTime(timestamp).toString(DateTimeFormat.forPattern(TIME_FORMAT));
  }
}
