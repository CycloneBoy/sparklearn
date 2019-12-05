package com.cycloneboy.bigdata.communication.kv.key;

import com.cycloneboy.bigdata.communication.kv.base.BaseDimension;
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
public class DateDimension extends BaseDimension {

  private Integer id;
  private Integer year;
  private Integer month;
  private Integer day;

  public DateDimension(Integer year, Integer month, Integer day) {
    super();
    this.year = year;
    this.month = month;
    this.day = day;
  }

  @Override
  public int compareTo(BaseDimension o) {
    if (this == o) return 0;
    if (o == null || getClass() != o.getClass()) return -1;

    DateDimension that = (DateDimension) o;

    int result = this.year.compareTo(that.year);

    if (result != 0) return result;

    result = this.month.compareTo(that.month);
    if (result != 0) return result;

    return this.day.compareTo(that.day);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
    out.writeInt(year);
    out.writeInt(month);
    out.writeInt(day);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readInt();
    year = in.readInt();
    month = in.readInt();
    day = in.readInt();
  }
}
