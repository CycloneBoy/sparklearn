package com.cycloneboy.bigdata.hivelearn.common;

import java.util.Date;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Create by  sl on 2020-06-20 11:53
 */
@Description(name = "zodiac",
    value = "_FUNC_(date) - from the input date string "
        + " or separate month and day arguments, return teh sign of the Zodiac.",
    extended = "Example:\n"
        + "> SELECT _FUNC_(date_string) from src;\n"
        + "> SELECT _FUNC_(month,day) from src;"
)
public class UDFZodiacSign extends UDF {

  //日期的输入格式固定为:yyyy-MM-dd
  public final static DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormat
      .forPattern("yyyy-MM-dd");


  public UDFZodiacSign() {
  }

  public String evaluate(Date bday) {
    return this.evaluate(bday.getMonth() + 1, bday.getDate());
  }

  public String evaluate(String birthday) {
    DateTime dateTime = null;
    try {
      dateTime = DateTime.parse(birthday, DEFAULT_DATE_FORMATTER);
    } catch (Exception e) {
      return null;
    }

    return evaluate(dateTime.toDate());

  }

  private String evaluate(Integer month, Integer day) {
    String[] zodiacArray = {"魔羯座", "水瓶座", "双鱼座", "白羊座", "金牛座", "双子座", "巨蟹座", "狮子座",
        "处女座", "天秤座", "天蝎座", "射手座"};
    int[] splitDay = {19, 18, 20, 20, 20, 21, 22, 22, 22, 22, 21, 21}; // 两个星座分割日
    int index = month;
    // 所查询日期在分割日之前，索引-1，否则不变
    if (day <= splitDay[month - 1]) {
      index = index - 1;
    } else if (month == 12) {
      index = 0;
    }
    // 返回索引指向的星座string
    return zodiacArray[index];

  }

  public static void main(String[] args) {
    UDFZodiacSign udfZodiacSignCn = new UDFZodiacSign();
    System.out.println("1990-11-02:     " + udfZodiacSignCn.evaluate("1990-11-02"));
    //错误格式的日期，返回值为null
    System.out.println(udfZodiacSignCn.evaluate("19901102"));
    System.out.println("2000-11-02:     " + udfZodiacSignCn.evaluate("2000-11-02"));
    System.out.println("2000-01-02:     " + udfZodiacSignCn.evaluate("2000-01-02"));

  }

}
