package com.cycloneboy.bigdata.hadoop.mapreduce.flow;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-10-19 15:57 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

  private Text phone = new Text();
  private FlowBean flow = new FlowBean();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] fields = value.toString().split("\t");
    phone.set(fields[1]);
    long upFlow = Long.parseLong(fields[fields.length - 3]);
    long downFlow = Long.parseLong(fields[fields.length - 2]);
    flow.set(upFlow, downFlow);

    context.write(phone, flow);
  }
}
