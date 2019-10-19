package com.cycloneboy.bigdata.hadoop.mapreduce.inputformat;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** Create by sl on 2019-10-19 17:44 自定义RR，处理一个文件；把这个文件直接读成一个KV值 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

  private boolean notRead = true;
  private BytesWritable value = new BytesWritable();
  private Text key = new Text();

  private FSDataInputStream inputStream;
  private FileSplit fs;

  /**
   * 初始化方法，框架会在开始的时候调用一次
   *
   * @param split
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    fs = (FileSplit) split;
    // 2 获取文件路径
    Path path = fs.getPath();
    FileSystem fileSystem = path.getFileSystem(context.getConfiguration());

    inputStream = fileSystem.open(path);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (notRead) {

      // 7 设置输出key
      key.set(this.fs.getPath().getName());

      // 1 定义缓冲区
      byte[] buf = new byte[(int) fs.getLength()];

      // 4 读取文件内容
      inputStream.read(buf, 0, buf.length);

      // 5 输出文件内容
      value.set(buf, 0, buf.length);

      notRead = false;
      return true;
    }

    return false;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return notRead ? 0 : 1;
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeStream(inputStream);
  }
}
