package com.cycloneboy.bigdata.hbaselearn.copy;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Create by sl on 2019-12-02 14:50
 *
 * <p>TableMapper<KEYOUT, VALUEOUT> extends Mapper <ImmutableBytesWritable, Result, KEYOUT,VALUEOUT>
 * { <br>
 * 1.从HBase表逐行读取数据,然后以Result形式返回; <br>
 * 2.默认 Mapper 的输入KV类型已经固定,唯一需要自定义的是输出KV类型 *
 */
public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
    Put put = new Put(key.get());

    for (Cell cell : value.listCells()) {
      // 添加/克隆列族:info
      if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
        // 添加/克隆列：name
        if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
          // 将该列cell加入到put对象中
          put.add(cell);
          // 添加/克隆列:color
        } else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
          // 向该列cell加入到put对象中
          put.add(cell);
        }
      }
    }
    // 将从fruit读取到的每行数据写入到context中作为map的输出
    context.write(key, put);
  }
}
