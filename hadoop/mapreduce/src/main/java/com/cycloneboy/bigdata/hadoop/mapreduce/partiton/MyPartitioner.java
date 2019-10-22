package com.cycloneboy.bigdata.hadoop.mapreduce.partiton;

import com.cycloneboy.bigdata.hadoop.mapreduce.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/** Create by sl on 2019-10-22 20:58 */
public class MyPartitioner extends Partitioner<Text, FlowBean> {

  @Override
  public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
    String phone = text.toString();

    switch (phone.substring(0, 3)) {
      case "135":
        return 0;
      case "136":
        return 1;
      case "137":
        return 2;
      case "138":
        return 3;
      default:
        return 4;
    }
  }
}
