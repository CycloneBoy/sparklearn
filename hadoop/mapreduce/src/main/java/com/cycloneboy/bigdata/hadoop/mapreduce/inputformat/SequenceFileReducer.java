package com.cycloneboy.bigdata.hadoop.mapreduce.inputformat;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Create by sl on 2019-10-19 18:28 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

  @Override
  protected void reduce(Text key, Iterable<BytesWritable> values, Context context)
      throws IOException, InterruptedException {
    context.write(key, values.iterator().next());
  }
}
