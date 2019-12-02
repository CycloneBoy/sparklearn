package com.cycloneboy.bigdata.hbaselearn.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Create by sl on 2019-12-02 19:09 */
@Slf4j
public class Txt2FruitRunner extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    Job job = Job.getInstance(conf, this.getClass().getSimpleName());
    job.setJarByClass(Txt2FruitRunner.class);

    Path path = new Path("hdfs://localhost:9000/input_fruit/fruit.tsv");
    FileInputFormat.addInputPath(job, path);

    job.setMapperClass(ReadFruitFromHDFSMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);
    job.setNumReduceTasks(1);

    TableMapReduceUtil.initTableReducerJob("fruit_mr2", WriteFruitMRFromTxtReducer.class, job);

    boolean result = job.waitForCompletion(true);
    if (result) {
      log.info("success");
    } else {
      log.info("failed");
    }

    return result ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();

    int status = ToolRunner.run(conf, new Txt2FruitRunner(), args);
    System.exit(status);
  }
}
