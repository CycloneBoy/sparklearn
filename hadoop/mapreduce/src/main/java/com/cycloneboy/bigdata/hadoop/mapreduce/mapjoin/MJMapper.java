package com.cycloneboy.bigdata.hadoop.mapreduce.mapjoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Create by sl on 2019-10-23 21:05 */
public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  private Map<String, String> pMap = new HashMap<>();
  private Text k = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    URI[] cacheFiles = context.getCacheFiles();
    String path = cacheFiles[0].getPath().toString();

    FileSystem fileSystem = FileSystem.get(context.getConfiguration());
    //    FSDataInputStream bufferedReader = fileSystem.open(new Path(path));
    BufferedReader bufferedReader =
        new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

    String line;
    //    while (StringUtils.isNoneEmpty(line = bufferedReader.readLine())) {
    while (StringUtils.isNoneEmpty(line = bufferedReader.readLine())) {
      String[] fields = line.split("\t");
      pMap.put(fields[0], fields[1]);
    }
    IOUtils.closeStream(bufferedReader);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] fields = value.toString().split("\t");
    String pname = pMap.get(fields[1]);
    if (pname == null) {
      pname = "NULL";
    }

    k.set(fields[0] + "\t" + pname + "\t" + fields[2]);
    context.write(k, NullWritable.get());
  }
}
