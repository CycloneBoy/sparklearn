package com.cycloneboy.bigdata.hadoop.mapreduce.videoetl;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-11-01 10:15 */
public class VideoETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
  private Text k = new Text();

  private StringBuilder sb = new StringBuilder();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();

    String result = handleLine(line);
    if (result == null) {
      context.getCounter("ETL", "False").increment(1);
    } else {
      context.getCounter("ETL", "True").increment(1);
      k.set(result);
      context.write(k, NullWritable.get());
    }
  }

  /**
   * ETL方法,处理掉长度不够的数据,并且吧数据形式做转换
   *
   * @param line 输入的行
   * @return 处理后的行
   */
  private String handleLine(String line) {
    String[] fields = line.split("\t");
    if (fields.length < 9) {
      return null;
    }

    sb.delete(0, sb.length());

    fields[3] = fields[3].replace(" ", "");

    for (int i = 0; i < fields.length; i++) {
      if (i == fields.length - 1) {
        sb.append(fields[i]);
      } else if (i < 9) {
        sb.append(fields[i]).append("\t");
      } else {
        sb.append(fields[i]).append("&");
      }
    }
    return sb.toString();
  }
}
