package com.cycloneboy.bigdata.kafka.data.serialization;

import com.cycloneboy.bigdata.kafka.data.common.Constants;
import com.cycloneboy.bigdata.kafka.data.model.Company;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

/** Create by sl on 2020-01-18 15:16 */
public class CompanySerializer implements Serializer<Company> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] serialize(String topic, Company data) {
    if (data == null) {
      return null;
    }

    byte[] name, address;

    try {
      if (data.getName() != null) {
        name = data.getName().getBytes(Constants.DEFAULT_CHARSET);
      } else {
        name = new byte[0];
      }
      if (data.getAddress() != null) {
        address = data.getAddress().getBytes(Constants.DEFAULT_CHARSET);
      } else {
        address = new byte[0];
      }

      ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
      buffer.putInt(name.length);
      buffer.put(name);
      buffer.putInt(address.length);
      buffer.put(address);
      return buffer.array();

    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return new byte[0];
  }
}
