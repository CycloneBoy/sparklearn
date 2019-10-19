package com.cycloneboy.bigdata.hadoop.mapreduce.flow;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Create by sl on 2019-10-19 15:57 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

  private FlowBean sumFlow = new FlowBean();

  @Override
  protected void reduce(Text key, Iterable<FlowBean> values, Context context)
      throws IOException, InterruptedException {
    long sumUpFlow = 0;
    long sumDownFlow = 0;

    // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
    for (FlowBean value : values) {
      sumUpFlow += value.getUpFlow();
      sumDownFlow += value.getDownFlow();
    }

    // 2 封装对象
    sumFlow.set(sumUpFlow, sumDownFlow);

    // 3 写出
    context.write(key, sumFlow);
  }
}
