package com.cycloneboy.bigdata.hadoop.mapreduce.outputformat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Create by sl on 2019-10-23 18:17
 * 过滤输入的log日志，包含atguigu的网站输出到e:/atguigu.log，不包含atguigu的网站输出到e:/other.log
 */
public class OutputDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 1 获取Job对象
    Job job = Job.getInstance(new Configuration());

    // 2 设置类路径
    job.setJarByClass(OutputDriver.class);

    job.setOutputFormatClass(MyOutputFormat.class);
    // 5 设置输入输出路径
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 6 提交
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
