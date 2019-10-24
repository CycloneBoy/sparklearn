package com.cycloneboy.bigdata.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Create by sl on 2019-10-24 10:40 */
public class FlowComparator extends WritableComparator {

  protected FlowComparator() {
    super(FlowBean.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    return 0;
  }
}
