package com.cycloneboy.bigdata.hadoop.mapreduce.invertindex;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Create by sl on 2019-10-24 10:12 */
public class IIReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

  private IntWritable v = new IntWritable();

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable value : values) {
      sum += value.get();
    }
    v.set(sum);

    context.write(key, v);
  }
}
