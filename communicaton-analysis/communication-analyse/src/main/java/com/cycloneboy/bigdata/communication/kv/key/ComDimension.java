package com.cycloneboy.bigdata.communication.kv.key;

import com.cycloneboy.bigdata.communication.kv.base.BaseDimension;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** Create by sl on 2019-12-05 14:10 */
@NoArgsConstructor
@Getter
@Setter
@ToString
public class ComDimension extends BaseDimension {

  private String id;
  private ContactDimension contactDimension = new ContactDimension();
  private DateDimension dateDimension = new DateDimension();

  public ComDimension(ContactDimension contactDimension, DateDimension dateDimension) {
    this.contactDimension = contactDimension;
    this.dateDimension = dateDimension;
    this.id = contactDimension.getId() + "_" + dateDimension.getId();
  }

  @Override
  public int compareTo(BaseDimension o) {
    ComDimension thatComDimension = (ComDimension) o;
    int result = dateDimension.compareTo(thatComDimension.getDateDimension());
    if (result != 0) return result;

    return contactDimension.compareTo(thatComDimension.getContactDimension());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    contactDimension.write(out);
    dateDimension.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    contactDimension.readFields(in);
    dateDimension.readFields(in);
  }
}
