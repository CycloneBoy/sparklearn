package com.cycloneboy.bigdata.hadoop.mapreduce.order;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/** Create by sl on 2019-10-22 23:38 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

  @Override
  protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {
    Iterator<NullWritable> iterator = values.iterator();
    for (int i = 0; i < 2; i++) {
      if (iterator.hasNext()) {
        context.write(key, iterator.next());
      }
    }
  }
}
