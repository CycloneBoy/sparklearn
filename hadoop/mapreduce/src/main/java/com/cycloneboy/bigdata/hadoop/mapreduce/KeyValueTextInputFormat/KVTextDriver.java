package com.cycloneboy.bigdata.hadoop.mapreduce.KeyValueTextInputFormat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Create by sl on 2019-10-19 17:20 1． 需求 统计输入文件中每一行的第一个单词相同的行数。 （1）输入数据 banzhang ni hao xihuan
 * hadoop banzhang banzhang ni hao xihuan hadoop banzhang （2）期望结果数据 banzhang 2 xihuan 2
 */
public class KVTextDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    // 设置切割符
    conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

    Job job = Job.getInstance(conf);

    job.setJarByClass(KVTextDriver.class);
    job.setMapperClass(KVTextMapper.class);
    job.setReducerClass(KVTextReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // 5 设置输入输出路径
    FileInputFormat.setInputPaths(job, new Path(args[0]));

    // 设置输入格式
    job.setInputFormatClass(KeyValueTextInputFormat.class);

    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    boolean result = job.waitForCompletion(true);

    System.exit(result ? 0 : 1);
  }
}
