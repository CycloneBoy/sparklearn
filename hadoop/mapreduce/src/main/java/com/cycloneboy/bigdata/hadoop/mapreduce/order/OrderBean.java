package com.cycloneboy.bigdata.hadoop.mapreduce.order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

/** Create by sl on 2019-10-22 23:37 */
@Getter
@Setter
@NoArgsConstructor
public class OrderBean implements WritableComparable<OrderBean> {

  private String orderId;
  private String productId;
  private double price;

  @Override
  public String toString() {
    return orderId + "\t" + productId + "\t" + price;
  }

  @Override
  public int compareTo(OrderBean o) {
    int compare = this.orderId.compareTo(o.orderId);

    // 根据价格降序排列
    if (compare == 0) {
      return Double.compare(o.price, this.price);
    } else {
      return compare;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(orderId);
    out.writeUTF(productId);
    out.writeDouble(price);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.orderId = in.readUTF();
    this.productId = in.readUTF();
    this.price = in.readDouble();
  }
}
