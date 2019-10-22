package com.cycloneboy.bigdata.hadoop.mapreduce.writablecomparable;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-10-22 21:20 */
public class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

  private Text phone = new Text();
  private FlowBean flow = new FlowBean();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 1 获取一行
    // 2 截取
    String[] fields = value.toString().split("\t");
    phone.set(fields[0]);

    // 3 封装对象
    long upFlow = Long.parseLong(fields[1]);
    long downFlow = Long.parseLong(fields[2]);
    flow.set(upFlow, downFlow);

    // 输出
    context.write(flow, phone);
  }
}
