package com.cycloneboy.bigdata.hadoop.mapreduce.invertindex;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-10-24 10:12 */
public class IIMapper2 extends Mapper<LongWritable, Text, Text, Text> {

  private Text k = new Text();
  private Text v = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split("--");
    k.set(split[0]);

    String[] fields = split[1].split("\t");
    v.set(fields[0] + "-->" + fields[1]);

    context.write(k, v);
  }
}
