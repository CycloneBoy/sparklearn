package com.cycloneboy.bigdata.hadoop.mapreduce.inputformat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/** Create by sl on 2019-10-19 18:30 */
public class SequenceFileDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    //    args = new String[]{""}

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf);

    job.setJarByClass(SequenceFileDriver.class);
    job.setMapperClass(SequenceFileMapper.class);
    job.setReducerClass(SequenceFileReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BytesWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);

    // 设置输入格式
    job.setInputFormatClass(WholeFileInputFormat.class);

    // 设置输出格式
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    // 5 设置输入输出路径
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    boolean result = job.waitForCompletion(true);

    System.exit(result ? 0 : 1);
  }
}
