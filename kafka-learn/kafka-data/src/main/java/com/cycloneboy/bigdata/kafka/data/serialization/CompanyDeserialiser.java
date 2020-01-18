package com.cycloneboy.bigdata.kafka.data.serialization;

import com.cycloneboy.bigdata.kafka.data.common.Constants;
import com.cycloneboy.bigdata.kafka.data.model.Company;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/** Create by sl on 2020-01-18 16:47 */
public class CompanyDeserialiser implements Deserializer<Company> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public Company deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    if (data.length < 8) {
      throw new SerializationException(
          "Size of data received by CompanyDeserialiser is shorter than expected");
    }

    ByteBuffer buffer = ByteBuffer.wrap(data);
    int nameLen, addressLen;
    String name = null, address = null;

    nameLen = buffer.getInt();
    byte[] nameBytes = new byte[nameLen];
    buffer.get(nameBytes);

    addressLen = buffer.getInt();
    byte[] addressBytes = new byte[addressLen];
    buffer.get(addressBytes);

    try {
      name = new String(nameBytes, Constants.DEFAULT_CHARSET);
      address = new String(addressBytes, Constants.DEFAULT_CHARSET);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    return new Company(name, address);
  }

  @Override
  public void close() {}
}
