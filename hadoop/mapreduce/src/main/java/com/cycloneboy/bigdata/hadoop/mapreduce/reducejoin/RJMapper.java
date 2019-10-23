package com.cycloneboy.bigdata.hadoop.mapreduce.reducejoin;

import com.cycloneboy.bigdata.hadoop.mapreduce.bean.OrderBean;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** Create by sl on 2019-10-23 20:44 */
public class RJMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

  private OrderBean orderBean = new OrderBean();

  private String filename;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    FileSplit fs = (FileSplit) context.getInputSplit();
    filename = fs.getPath().getName();
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] fields = value.toString().split("\t");

    if (filename.equals("order.txt")) {
      orderBean.setId(fields[0]);
      orderBean.setPid(fields[1]);
      orderBean.setAmount(Integer.parseInt(fields[2]));
      orderBean.setPname("");
    } else {
      orderBean.setPid(fields[0]);
      orderBean.setPname(fields[1]);
      orderBean.setId("");
      orderBean.setAmount(0);
    }

    context.write(orderBean, NullWritable.get());
  }
}
