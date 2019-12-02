package com.cycloneboy.bigdata.hbaselearn.hdfs;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-12-02 18:58 */
public class ReadFruitFromHDFSMapper
    extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String lineValue = value.toString();
    String[] values = lineValue.split("\t");

    String rowKey = values[0];
    String name = values[1];
    String color = values[2];

    // 初始化rowKey
    ImmutableBytesWritable rowKeyWritbale = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

    // 初始化put对象
    Put put = new Put(Bytes.toBytes(rowKey));

    // 参数分别:列族、列、值
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(color));
    context.write(rowKeyWritbale, put);
  }
}
