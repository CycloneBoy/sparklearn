package com.cycloneboy.bigdata.hadoop.mapreduce.invertindex;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Create by sl on 2019-10-24 10:11
 *
 * <p>有大量的文本（文档、网页），需要建立搜索索引
 */
public class IIDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 1 获取Job对象
    Job job1 = Job.getInstance(new Configuration());

    // 2 设置类路径
    job1.setJarByClass(IIDriver.class);

    // 3 设置Mapper和Reducer
    job1.setMapperClass(IIMpper1.class);
    job1.setReducerClass(IIReducer1.class);

    // 4 设置输入输出类型
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    // 5 设置输入输出路径
    FileInputFormat.setInputPaths(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    // 6 提交
    boolean result = job1.waitForCompletion(true);

    if (result) {
      // 1 获取Job对象
      Job job2 = Job.getInstance(new Configuration());

      // 2 设置类路径
      job2.setJarByClass(IIDriver.class);

      // 3 设置Mapper和Reducer
      job2.setMapperClass(IIMapper2.class);
      job2.setReducerClass(IIReducer2.class);

      // 4 设置输入输出类型
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);

      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);

      // 5 设置输入输出路径
      //      FileInputFormat.setInputPaths(job2, new Path(args[0]));
      //      FileOutputFormat.setOutputPath(job2, new Path(args[1]));
      FileInputFormat.setInputPaths(job2, new Path("/home/sl/workspace/bigdata/output"));
      FileOutputFormat.setOutputPath(job2, new Path("/home/sl/workspace/bigdata/output2"));

      // 6 提交
      boolean b2 = job2.waitForCompletion(true);
      System.exit(b2 ? 0 : 1);
    }
  }
}
