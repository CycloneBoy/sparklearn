package com.cycloneboy.bigdata.hadoop.mapreduce.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

/** Create by sl on 2019-10-23 20:39 */
@Getter
@Setter
@NoArgsConstructor
public class OrderBean implements WritableComparable<OrderBean> {

  private String id;
  private String pid;
  private int amount;
  private String pname;

  @Override
  public String toString() {
    return id + "\t" + pname + "\t" + amount;
  }

  @Override
  public int compareTo(OrderBean o) {
    int compare = this.pid.compareTo(o.pid);

    // 根据价格降序排列
    if (compare == 0) {
      return o.pname.compareTo(this.pname);
    } else {;
      return compare;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(id);
    out.writeUTF(pid);
    out.writeInt(amount);
    out.writeUTF(pname);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readUTF();
    this.pid = in.readUTF();
    this.amount = in.readInt();
    this.pname = in.readUTF();
  }
}
