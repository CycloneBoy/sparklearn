package com.cycloneboy.bigdata.hadoop.mapreduce.topn;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-10-24 10:40 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

  private Text phone = new Text();
  private FlowBean flow = new FlowBean();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] fields = value.toString().split("\t");
    phone.set(fields[0]);
    long upFlow = Long.parseLong(fields[1]);
    long downFlow = Long.parseLong(fields[2]);
    flow.set(upFlow, downFlow);

    context.write(flow, phone);
  }
}
