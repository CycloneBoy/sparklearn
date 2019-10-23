package com.cycloneboy.bigdata.hadoop.mapreduce.reducejoin;

import com.cycloneboy.bigdata.hadoop.mapreduce.bean.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Create by sl on 2019-10-23 20:44 */
public class RJComparator extends WritableComparator {

  protected RJComparator() {
    super(OrderBean.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    OrderBean oa = (OrderBean) a;
    OrderBean ob = (OrderBean) b;
    return oa.getPid().compareTo(ob.getPid());
  }
}
