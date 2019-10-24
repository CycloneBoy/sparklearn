package com.cycloneboy.bigdata.hadoop.mapreduce.friend;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Create by sl on 2019-10-24 11:00 <br>
 * 找博客好友<br>
 *
 * <p>需求 以下是博客的好友列表数据，冒号前是一个用户，冒号后是该用户的所有好友（数据中的好友关系是单向的） <br>
 * 求出哪些人两两之间有共同好友，及他俩的共同好友都有谁？
 */
public class FFDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 1 获取Job对象
    Job job1 = Job.getInstance(new Configuration());

    // 2 设置类路径
    job1.setJarByClass(FFDriver.class);

    // 3 设置Mapper和Reducer
    job1.setMapperClass(FFMapper1.class);
    job1.setReducerClass(FFReducer1.class);

    // 4 设置输入输出类型
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    // 5 设置输入输出路径
    FileInputFormat.setInputPaths(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    // 6 提交
    boolean result = job1.waitForCompletion(true);

    if (result) {
      // 1 获取Job对象
      Job job2 = Job.getInstance(new Configuration());

      // 2 设置类路径
      job2.setJarByClass(FFDriver.class);

      // 3 设置Mapper和Reducer
      job2.setMapperClass(FFMapper2.class);
      job2.setReducerClass(FFReducer2.class);

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
