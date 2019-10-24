package com.cycloneboy.bigdata.hadoop.mapreduce.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Create by sl on 2019-10-24 09:11
 *
 * <p>测试编解码
 *
 * <p>DEFLATE org.apache.hadoop.io.compress.DefaultCodec gzip
 * org.apache.hadoop.io.compress.GzipCodec bzip2 org.apache.hadoop.io.compress.BZip2Code c
 */
@Slf4j
public class TestCompress {

  public static void main(String[] args) throws IOException, ClassNotFoundException {

    //    compress("/home/sl/workspace/bigdata/edits.xml",
    // "org.apache.hadoop.io.compress.BZip2Codec");
    decompress("/home/sl/workspace/bigdata/atest/edits.xml.bz2");
  }

  /**
   * 压缩
   *
   * @param filename
   * @param method
   */
  private static void compress(String filename, String method)
      throws IOException, ClassNotFoundException {
    // 1 获取输入流
    FileInputStream fis = new FileInputStream(new File(filename));

    Class<?> codecClass = Class.forName(method);

    CompressionCodec codec =
        (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());

    // 2 获取输出流
    FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));

    CompressionOutputStream cos = codec.createOutputStream(fos);

    // 3 流对拷
    IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);

    // 4 关闭资源
    cos.close();
    fos.close();
    fis.close();
  }

  /**
   * 解压缩
   *
   * @param filename
   */
  private static void decompress(String filename) throws IOException {

    // 1 教研是否能够解压缩
    CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());

    CompressionCodec codec = factory.getCodec(new Path(filename));

    if (codec == null) {
      log.info("cannot find codec for file : {}", filename);
      return;
    }

    // 2 获取输入流
    CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(filename)));

    // 3 获取输出流
    FileOutputStream fos = new FileOutputStream(new File(filename + ".decoded"));

    // 4 流对拷
    IOUtils.copyBytes(cis, fos, 1024 * 1024 * 5, false);

    // 5 关闭资源
    cis.close();
    fos.close();
  }
}
