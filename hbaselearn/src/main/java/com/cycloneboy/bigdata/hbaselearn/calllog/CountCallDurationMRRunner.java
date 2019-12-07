package com.cycloneboy.bigdata.hbaselearn.calllog;

import com.cycloneboy.bigdata.hbaselearn.calllog.kv.key.ComDimension;
import com.cycloneboy.bigdata.hbaselearn.calllog.kv.value.CountCallDurationValue;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Create by sl on 2019-12-02 15:04
 *
 * <p>目标：将fruit表中的一部分数据，通过MR迁入到fruit_mr表中
 */
@Slf4j
public class CountCallDurationMRRunner extends Configured implements Tool {

  //  private static Configuration conf;
  //
  //  static {
  //    // 使用HBaseConfiguration的单例方法实例化
  //    conf = HBaseConfiguration.create();
  //    conf.set("hbase.zookeeper.quorum", "localhost");
  //    conf.set("hbase.zookeeper.property.clientPort", "2181");
  //  }

  /**
   * 初始化mapper
   *
   * @param job
   */
  private void initHbaseInputConfig(Job job) {
    Connection connection = null;
    Admin admin = null;

    try {
      String tableName = "call:calllog";
      connection = ConnectionFactory.createConnection(job.getConfiguration());
      admin = connection.getAdmin();

      if (!admin.tableExists(TableName.valueOf(tableName)))
        throw new RuntimeException("无法找到对应的hbase表格:" + tableName);
      Scan scan = new Scan();
      TableMapReduceUtil.initTableMapperJob(
          tableName,
          scan,
          CountCallDurationMapper.class,
          ComDimension.class,
          Text.class,
          job,
          true);

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (admin != null) {
          admin.close();
        }
        if (connection != null && !connection.isClosed()) {
          connection.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();

    // 1.获取Job实例,当本地的 hbase  或  hadoop 相关的 xml 配置需要覆盖集群配置时,创建job需要指定 conf

    Job job = Job.getInstance(conf, this.getClass().getSimpleName());
    // 2.注册驱动类,提交yarn集群执行 mr 程序是必须的,若在本地运行则可随意

    job.setJarByClass(CountCallDurationMRRunner.class);

    /*3.org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil 新版本API,使用scan
     * org.apache.hadoop.hbase.mapred.TableMapReduceUtil 老版本API使用Columns
     */
    Scan scan = new Scan();

    // 4.设置使用缓存,但不设置缓存块
    scan.setCacheBlocks(false);
    // 每500条写提交写入一次
    scan.setCaching(500);

    TableMapReduceUtil.initTableMapperJob(
        "call:calllog", scan, CountCallDurationMapper.class, ComDimension.class, Text.class, job);

    //    initHbaseInputConfig(job);

    job.setReducerClass(CountCallDurationReducer.class);
    job.setOutputKeyClass(ComDimension.class);
    job.setOutputValueClass(CountCallDurationValue.class);
    //    job.setOutputFormatClass(MysqlOutputFormat.class);

    //    job.setNumReduceTasks(1);

    Path path = new Path("hdfs://localhost:9000/calllog1");
    FileOutputFormat.setOutputPath(job, path);

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

    int status = ToolRunner.run(conf, new CountCallDurationMRRunner(), args);
    System.exit(status);
  }
}
