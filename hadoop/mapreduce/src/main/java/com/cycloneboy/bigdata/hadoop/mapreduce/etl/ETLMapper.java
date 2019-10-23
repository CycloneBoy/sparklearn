package com.cycloneboy.bigdata.hadoop.mapreduce.etl;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-10-23 21:58 */
public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] fields = value.toString().split("\t");

    if (fields.length > 11) {
      context.write(value, NullWritable.get());
      context.getCounter("ETL", "True").increment(1);
    } else {
      context.getCounter("ETL", "False").increment(1);
    }
  }
}
