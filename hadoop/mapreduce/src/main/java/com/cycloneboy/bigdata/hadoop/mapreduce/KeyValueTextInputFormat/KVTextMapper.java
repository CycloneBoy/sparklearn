package com.cycloneboy.bigdata.hadoop.mapreduce.KeyValueTextInputFormat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-10-19 17:20 */
public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {

  private LongWritable v = new LongWritable(1);

  @Override
  protected void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {

    context.write(key, v);
  }
}
