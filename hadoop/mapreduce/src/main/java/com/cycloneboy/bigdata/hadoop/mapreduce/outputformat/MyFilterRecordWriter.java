package com.cycloneboy.bigdata.hadoop.mapreduce.outputformat;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Create by sl on 2019-10-23 18:25 */
public class MyFilterRecordWriter extends RecordWriter<LongWritable, Text> {

  private FSDataOutputStream atguigu;
  private FSDataOutputStream other;

  public void initialize(TaskAttemptContext job) throws IOException {
    String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    atguigu = fileSystem.create(new Path(outdir + "/atguigu.log"));
    other = fileSystem.create(new Path(outdir + "/other.log"));
  }

  @Override
  public void write(LongWritable key, Text value) throws IOException, InterruptedException {
    String out = value.toString() + "\n";
    if (out.contains("atguigu")) {
      atguigu.write(out.getBytes());
    } else {
      other.write(out.getBytes());
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    IOUtils.closeStream(atguigu);
    IOUtils.closeStream(other);
  }
}
