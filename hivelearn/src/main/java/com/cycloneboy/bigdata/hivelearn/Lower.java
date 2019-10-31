package com.cycloneboy.bigdata.hivelearn;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/** Create by sl on 2019-10-31 18:22 */
@Description(
    name = "Lower",
    value = "_FUNC_(str) - from the input string" + "returns the value that is $str Lower case ",
    extended = "Example:\n" + " > SELECT _FUNC_(str) FROM src;")
public class Lower extends UDF {

  public String evaluate(String s) {
    if (s == null) {
      return null;
    }
    return s.toLowerCase();
  }
}
