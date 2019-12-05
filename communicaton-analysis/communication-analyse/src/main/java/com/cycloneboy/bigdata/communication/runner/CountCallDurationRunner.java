package com.cycloneboy.bigdata.communication.runner;

import com.cycloneboy.bigdata.communication.converter.MysqlOutputFormat;
import com.cycloneboy.bigdata.communication.kv.key.ComDimension;
import com.cycloneboy.bigdata.communication.kv.value.CountCallDurationValue;
import com.cycloneboy.bigdata.communication.mapper.CountCallDurationMapper;
import com.cycloneboy.bigdata.communication.reducer.CountCallDurationReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Create by sl on 2019-12-05 13:51 */
public class CountCallDurationRunner implements Tool {

  private Configuration conf = null;

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(CountCallDurationRunner.class);
    initHbaseInputConfig(job);
    initReducerOutputConfig(job);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * 初始化reducer
   *
   * @param job
   */
  private void initReducerOutputConfig(Job job) {
    job.setReducerClass(CountCallDurationReducer.class);
    job.setOutputKeyClass(ComDimension.class);
    job.setOutputValueClass(CountCallDurationValue.class);
    job.setOutputFormatClass(MysqlOutputFormat.class);
  }

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
  public void setConf(Configuration conf) {
    this.conf = HBaseConfiguration.create(conf);
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  public static void main(String[] args) {
    try {
      int result = ToolRunner.run(new CountCallDurationRunner(), args);
      System.exit(result);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
