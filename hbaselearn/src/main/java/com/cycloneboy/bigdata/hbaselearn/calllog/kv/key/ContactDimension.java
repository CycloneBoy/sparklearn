package com.cycloneboy.bigdata.hbaselearn.calllog.kv.key;

import com.cycloneboy.bigdata.hbaselearn.calllog.kv.base.BaseDimension;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Create by sl on 2019-12-05 14:10
 * +-----------+--------------+------+-----+---------+----------------+<br>
 * | Field | Type | Null | Key | Default | Extra |<br>
 * +-----------+--------------+------+-----+---------+----------------+ <br>
 * | id | int(11) | NO | PRI | NULL | auto_increment | | telephone | varchar(255) | NO | | NULL | |
 * <br>
 * | name | varchar(255) | NO | | NULL | |
 * +-----------+--------------+------+-----+---------+----------------+<br>
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class ContactDimension extends BaseDimension {

  private Integer id;
  private String telephone;
  private String name;

  public ContactDimension(String telephone, String name) {
    super();
    this.id = -1;
    this.telephone = telephone;
    this.name = name;
  }

  @Override
  public int compareTo(BaseDimension o) {
    if (this == o) return 0;
    if (o == null || getClass() != o.getClass()) return -1;

    ContactDimension that = (ContactDimension) o;

    int result = telephone.compareTo(that.getTelephone());
    if (result == 0) {
      return name.compareTo(that.getName());
    }
    return result;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
    out.writeUTF(telephone);
    out.writeUTF(name);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readInt();
    telephone = in.readUTF();
    name = in.readUTF();
  }
}
