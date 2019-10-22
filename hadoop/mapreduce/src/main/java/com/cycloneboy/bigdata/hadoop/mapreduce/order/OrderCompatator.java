package com.cycloneboy.bigdata.hadoop.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Create by sl on 2019-10-22 23:37 */
public class OrderCompatator extends WritableComparator {

  protected OrderCompatator() {
    super(OrderBean.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    OrderBean oa = (OrderBean) a;
    OrderBean ob = (OrderBean) b;
    return oa.getOrderId().compareTo(ob.getOrderId());
  }
}
