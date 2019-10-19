package com.cycloneboy.bigdata.hadoop.mapreduce.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

/** Create by sl on 2019-10-19 15:57 */
@Getter
@Setter
@NoArgsConstructor
public class FlowBean implements Writable {

  private long upFlow;
  private long downFlow;
  private long sumFlow;

  public void set(long upFlow, long downFlow) {
    this.upFlow = upFlow;
    this.downFlow = downFlow;
    this.sumFlow = upFlow + downFlow;
  }

  @Override
  public String toString() {
    return upFlow + "\t" + downFlow + "\t" + sumFlow;
  }

  /**
   * 序列化方法
   *
   * @param out 框架给我们提供的数据出口
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(upFlow);
    out.writeLong(downFlow);
    out.writeLong(sumFlow);
  }

  /**
   * 反序列方法
   *
   * @param in 框架提供的数据来源
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.upFlow = in.readLong();
    this.downFlow = in.readLong();
    this.sumFlow = in.readLong();
  }
}
