package com.cycloneboy.bigdata.hbaselearn.copy;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

/**
 * Create by sl on 2019-12-02 15:00
 *
 * <p>* TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation> *
 * 1.TableReducer的输出Value类型固定:Mutation抽象类,且Get,Put,Delete,Increment 都是其子类 <br>
 * 2.直接遍历逐个发送即可
 */
public class WriteFruitMRReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
      throws IOException, InterruptedException {
    // 读出来的每一行数据写入到fruit_mr表中
    for (Put put : values) {
      context.write(NullWritable.get(), put);
    }
  }
}
