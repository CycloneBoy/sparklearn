package com.cycloneboy.bigdata.hadoop.mapreduce.topn;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Create by sl on 2019-10-24 10:40 */
public class FlowDriver {

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // 1 获取Job对象
    Job job = Job.getInstance(new Configuration());

    // 2 设置类路径
    job.setJarByClass(FlowDriver.class);

    // 3 设置Mapper和Reducer
    job.setMapperClass(FlowMapper.class);
    job.setReducerClass(FlowReducer.class);

    // 4 设置输入输出类型
    job.setMapOutputKeyClass(FlowBean.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setGroupingComparatorClass(FlowComparator.class);

    // 5 设置输入输出路径
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 6 提交
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
