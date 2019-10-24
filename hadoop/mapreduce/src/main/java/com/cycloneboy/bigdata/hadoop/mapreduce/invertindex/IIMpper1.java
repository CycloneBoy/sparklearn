package com.cycloneboy.bigdata.hadoop.mapreduce.invertindex;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** Create by sl on 2019-10-24 10:11 */
public class IIMpper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

  private Text k = new Text();
  private IntWritable v = new IntWritable(1);

  private String filename;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    FileSplit fs = (FileSplit) context.getInputSplit();
    filename = fs.getPath().getName();
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] words = value.toString().split(" ");
    for (String word : words) {
      k.set(word + "--" + filename);
      context.write(k, v);
    }
  }
}
