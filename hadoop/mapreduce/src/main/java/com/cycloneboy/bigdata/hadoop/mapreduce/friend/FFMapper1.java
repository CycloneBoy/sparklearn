package com.cycloneboy.bigdata.hadoop.mapreduce.friend;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-10-24 11:00 */
public class FFMapper1 extends Mapper<LongWritable, Text, Text, Text> {

  private Text k = new Text();
  private Text v = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split(":");
    // 关注别人的人作为value
    v.set(split[0]);

    // 被关注的人作为key
    for (String man : split[1].split(",")) {
      k.set(man);
      context.write(k, v);
    }
  }
}
