package com.cycloneboy.bigdata.hbaselearn.calllog.kv.value;

import com.cycloneboy.bigdata.hbaselearn.calllog.kv.base.BaseValue;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** Create by sl on 2019-12-05 14:10 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class CountCallDurationValue extends BaseValue {

  private Integer callSum;
  private Integer callDurationSum;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(callSum);
    out.writeInt(callDurationSum);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    callSum = in.readInt();
    callDurationSum = in.readInt();
  }
}
