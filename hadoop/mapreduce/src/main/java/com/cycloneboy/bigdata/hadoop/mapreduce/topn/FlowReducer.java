package com.cycloneboy.bigdata.hadoop.mapreduce.topn;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Create by sl on 2019-10-24 10:40 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

  @Override
  protected void reduce(FlowBean key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    for (int i = 0; i < 10; i++) {
      if (iterator.hasNext()) {
        context.write(iterator.next(), key);
      }
    }
  }
}
