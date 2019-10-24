package com.cycloneboy.bigdata.hadoop.mapreduce.friend;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-10-24 11:00 */
public class FFMapper2 extends Mapper<LongWritable, Text, Text, Text> {

  private Text k = new Text();
  private Text v = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split("\t");

    // 获取被关注的人
    v.set(split[0]);

    String[] man = split[1].split(",");
    for (int i = 0; i < man.length; i++) {
      for (int j = i + 1; j < man.length; j++) {
        if (man[i].compareTo(man[j]) > 0) {
          k.set(man[j] + "-" + man[i]);
        } else {
          k.set(man[i] + "-" + man[j]);
        }

        context.write(k, v);
      }
    }
  }
}
