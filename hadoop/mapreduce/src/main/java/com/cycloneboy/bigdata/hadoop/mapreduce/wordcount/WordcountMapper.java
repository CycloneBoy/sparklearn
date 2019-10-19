package com.cycloneboy.bigdata.hadoop.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-10-19 09:39 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private Text k = new Text();
  private IntWritable v = new IntWritable(1);

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    // 1 获取第一行
    String line = value.toString();

    // 2 切割
    String[] words = line.split(" ");

    // 3 输出
    for (String word : words) {
      k.set(word);
      context.write(k, v);
    }
  }
}
