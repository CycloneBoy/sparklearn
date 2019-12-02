package com.cycloneboy.bigdata.hbaselearn.copy;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Create by sl on 2019-12-02 15:04
 *
 * <p>目标：将fruit表中的一部分数据，通过MR迁入到fruit_mr表中
 */
@Slf4j
public class Fruit2FruitMRRunner extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();

    // 1.获取Job实例,当本地的 hbase  或  hadoop 相关的 xml 配置需要覆盖集群配置时,创建job需要指定 conf

    Job job = Job.getInstance(conf, this.getClass().getSimpleName());
    // 2.注册驱动类,提交yarn集群执行 mr 程序是必须的,若在本地运行则可随意

    job.setJarByClass(Fruit2FruitMRRunner.class);

    /*3.org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil 新版本API,使用scan
     * org.apache.hadoop.hbase.mapred.TableMapReduceUtil 老版本API使用Columns
     */
    Scan scan = new Scan();

    // 4.设置使用缓存,但不设置缓存块
    scan.setCacheBlocks(false);
    // 每500条写提交写入一次
    scan.setCaching(500);

    TableMapReduceUtil.initTableMapperJob(
        args[0], scan, ReadFruitMapper.class, ImmutableBytesWritable.class, Put.class, job);

    TableMapReduceUtil.initTableReducerJob(args[1], WriteFruitMRReducer.class, job);

    job.setNumReduceTasks(1);

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

    int status = ToolRunner.run(conf, new Fruit2FruitMRRunner(), args);
    System.exit(status);
  }
}
