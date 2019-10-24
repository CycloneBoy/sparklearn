package com.cycloneboy.bigdata.hadoop.mapreduce.friend;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Create by sl on 2019-10-24 11:00 */
public class FFReducer1 extends Reducer<Text, Text, Text, Text> {

  private Text v = new Text();

  private StringBuilder sb = new StringBuilder();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    sb.delete(0, sb.length());
    for (Text value : values) {
      sb.append(value.toString()).append(",");
    }
    v.set(sb.toString());
    context.write(key, v);
  }
}
