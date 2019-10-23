package com.cycloneboy.bigdata.hadoop.mapreduce.outputformat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Create by sl on 2019-10-23 18:17 */
public class MyOutputFormat extends FileOutputFormat<LongWritable, Text> {

  @Override
  public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    MyFilterRecordWriter myFilterRecordWriter = new MyFilterRecordWriter();
    myFilterRecordWriter.initialize(job);
    return myFilterRecordWriter;
  }
}
