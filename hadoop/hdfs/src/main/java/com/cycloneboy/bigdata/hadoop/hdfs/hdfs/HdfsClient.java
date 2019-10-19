package com.cycloneboy.bigdata.hadoop.hdfs.hdfs;



import static com.cycloneboy.bigdata.hadoop.hdfs.common.Constants.HDFS_URI;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * Create by  sl on 2019-10-18 23:31
 *
 * hdfs的java client 操作
 */
@Slf4j
public class HdfsClient {

  @Test
  public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {

    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");

    // 2 创建目录
    fs.mkdirs(new Path("/1018/sl"));

    // 3 关闭资源
    fs.close();
  }

  @Test
  public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");

    // 2 上传文件
    fs.copyFromLocalFile(new Path("/home/sl/workspace/bigdata/test01.txt"),new Path("test01.txt"));

    // 3 关闭资源
    fs.close();

    log.info("test over");

  }

  @Test
  public void testCopyToLocalFile() throws IOException, URISyntaxException, InterruptedException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");

    // 2 执行下载文件
    // boolean delSrc 指是否将原文件删除
    // Path src 指要下载的文件路径
    // Path dst 指将文件下载到的路径
    // boolean useRawLocalFileSystem 是否开启文件校验
    fs.copyToLocalFile(false,new Path("test01.txt"),new Path("/home/sl/workspace/bigdata/test02.txt"),true);

    // 3 关闭资源
    fs.close();

    log.info("test over");
  }

  @Test
  public void testDelete() throws IOException, URISyntaxException, InterruptedException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");

    // 2 执行删除
    fs.delete(new Path("/user/cycloneboy/ppEnv.json"),true);

    // 3 关闭资源
    fs.close();

    log.info("test over");
  }

  @Test
  public void testRename() throws URISyntaxException, IOException, InterruptedException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");

    // 2 重命名
    fs.rename(new Path("/user/cycloneboy/test01.txt"),new Path("test03.txt"));

    // 3 关闭资源
    fs.close();

    log.info("test over");

  }

  @Test
  public void testListFileds() throws URISyntaxException, IOException, InterruptedException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");

    // 2 获取文件详情
    RemoteIterator<LocatedFileStatus> listFiles = fs
        .listFiles(new Path("/"), true);

    while (listFiles.hasNext()){
      LocatedFileStatus fileStatus = listFiles.next();

      // 输出详情
      // 文件名称
      log.info(fileStatus.getPath().getName());

      // 长度
      log.info("{}",fileStatus.getLen());

      // 权限
      log.info(fileStatus.getPermission().toString());

      // 分组
      log.info(fileStatus.getGroup());

      // 获取存储的块信息
      BlockLocation[] blockLocations = fileStatus.getBlockLocations();

      for (BlockLocation location : blockLocations) {
        String[] hosts = location.getHosts();

        for (String host : hosts) {
          log.info(host);
        }
      }

    }

    // 3 关闭资源
    fs.close();

    log.info("test over");

  }

  @Test
  public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");

    // 2 判断是文件还是文件夹
    FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

    for (FileStatus fileStatus : fileStatuses) {

      if(fileStatus.isFile()){
        log.info("f: {}",fileStatus.getPath().getName());
      }else {
        log.info("d: {}",fileStatus.getPath().getName());
      }
    }

    // 3 关闭资源
    fs.close();

    log.info("test over");
  }

  @Test
  public void putFileToHdfs() throws IOException, URISyntaxException, InterruptedException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");


    // 2 创建输出流
    FileInputStream fis = new FileInputStream(
        new File("/home/sl/workspace/bigdata/testhdfs01.txt"));

    // 3 创建输入流
    FSDataOutputStream fos = fs.create(new Path("/testhdfs01.txt"));

    // 4 流对拷贝
    IOUtils.copyBytes(fis,fos,configuration);

    // 5 关闭资源
    IOUtils.closeStream(fis);
    IOUtils.closeStream(fos);

    fs.close();
  }

  @Test
  public void getFileFromHdfs() throws IOException, URISyntaxException, InterruptedException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");


    // 2 获取输入流
    FSDataInputStream fis =fs.open(new Path("/testhdfs01.txt"));

    // 3 创建输出流
    FileOutputStream fos =   new FileOutputStream(
        new File("/home/sl/workspace/bigdata/testhdfs02.txt"));

    // 4 流对拷贝
    IOUtils.copyBytes(fis,fos,configuration);

    // 5 关闭资源
    IOUtils.closeStream(fis);
    IOUtils.closeStream(fos);

    fs.close();
  }

  /**
   * 下载第一块
   * @throws IOException
   * @throws URISyntaxException
   * @throws InterruptedException
   */
  @Test
  public void testReadFileSeek1() throws IOException, URISyntaxException, InterruptedException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");

    // 2 获取输入流
    FSDataInputStream fis =fs.open(new Path("/spark-2.4.3-bin-hadoop2.7.tgz"));

    // 3 创建输出流
    FileOutputStream fos =   new FileOutputStream(
        new File("/home/sl/workspace/bigdata/spark-2.4.3-bin-hadoop2.7.tgz.part1"));

    // 4 流对拷贝
    byte[] buf = new byte[1024];
    for (int i = 0; i < 1024 * 128; i++) {
      fis.read(buf);
      fos.write(buf);
    }

    // 5 关闭资源
    IOUtils.closeStream(fis);
    IOUtils.closeStream(fos);

    fs.close();

  }

  /**
   * 下载第二块
   * @throws IOException
   * @throws URISyntaxException
   * @throws InterruptedException
   */
  @Test
  public void testReadFileSeek2() throws IOException, URISyntaxException, InterruptedException {
    // 1 获取文件系统
    Configuration configuration = new Configuration();

    // 配置在集群上运行
    configuration.set("fs.defaultFS",HDFS_URI);

    FileSystem fs = FileSystem
        .get(new URI(HDFS_URI), configuration, "cycloneboy");

    // 2 获取输入流
    FSDataInputStream fis =fs.open(new Path("/spark-2.4.3-bin-hadoop2.7.tgz"));

    // 3 定位输入文件的位置
    fis.seek(1024*1024*128);

    // 4 创建输出流
    FileOutputStream fos =   new FileOutputStream(
        new File("/home/sl/workspace/bigdata/spark-2.4.3-bin-hadoop2.7.tgz.part2"));

    // 4 流对拷贝
    IOUtils.copyBytes(fis,fos,configuration);

    // 5 关闭资源
    IOUtils.closeStream(fis);
    IOUtils.closeStream(fos);

    fs.close();

  }
}
