package com.cycloneboy.bigdata.hbaselearn.hdfs;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

/** Create by sl on 2019-12-02 19:07 */
public class WriteFruitMRFromTxtReducer
    extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
      throws IOException, InterruptedException {
    // 读出来的每一行数据写入到fruit_hdfs表中
    for (Put value : values) {
      context.write(NullWritable.get(), value);
    }
  }
}
