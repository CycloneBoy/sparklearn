package com.cycloneboy.bigdata.hadoop.mapreduce.writablecomparable;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Create by sl on 2019-10-22 21:20 */
public class SortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

  @Override
  protected void reduce(FlowBean key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    // 循环输出，避免总流量相同情况
    for (Text value : values) {
      context.write(value, key);
    }
  }
}
