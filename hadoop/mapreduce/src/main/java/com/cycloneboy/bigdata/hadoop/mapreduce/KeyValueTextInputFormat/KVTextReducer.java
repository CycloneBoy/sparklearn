package com.cycloneboy.bigdata.hadoop.mapreduce.KeyValueTextInputFormat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Create by sl on 2019-10-19 17:20 */
public class KVTextReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  private LongWritable v = new LongWritable(1);

  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long sum = 0L;

    // 1 汇总统计
    for (LongWritable value : values) {
      sum += value.get();
    }

    v.set(sum);

    // 2 输出
    context.write(key, v);
  }
}
