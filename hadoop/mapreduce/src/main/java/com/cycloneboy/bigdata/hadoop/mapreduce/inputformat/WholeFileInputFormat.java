package com.cycloneboy.bigdata.hadoop.mapreduce.inputformat;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/** Create by sl on 2019-10-19 17:39 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {

  /**
   * 小文件不允许拆分
   *
   * @param context
   * @param filename
   * @return
   */
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<Text, BytesWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

    WholeRecordReader recordReader = new WholeRecordReader();
    //        recordReader.initialize(split, context);
    return recordReader;
    //    return new WholeFileRecordReader();
  }
}
