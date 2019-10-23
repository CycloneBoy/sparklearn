package com.cycloneboy.bigdata.hadoop.mapreduce.mapjoin;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Create by sl on 2019-10-23 21:05 */
public class MJDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 1 获取Job对象
    Job job = Job.getInstance(new Configuration());

    // 2 设置类路径
    job.setJarByClass(MJDriver.class);

    // 3 设置Mapper和Reducer
    job.setMapperClass(MJMapper.class);
    job.setNumReduceTasks(0);

    job.addCacheFile(URI.create("file:///home/sl/workspace/bigdata/order/pd.txt"));
    // 5 设置输入输出路径
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 6 提交
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
