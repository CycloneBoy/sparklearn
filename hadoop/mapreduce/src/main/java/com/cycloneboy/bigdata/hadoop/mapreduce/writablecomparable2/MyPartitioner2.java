package com.cycloneboy.bigdata.hadoop.mapreduce.writablecomparable2;

import com.cycloneboy.bigdata.hadoop.mapreduce.writablecomparable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/** Create by sl on 2019-10-22 20:58 */
public class MyPartitioner2 extends Partitioner<FlowBean, Text> {

  @Override
  public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
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
