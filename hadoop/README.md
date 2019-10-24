# Hadoop learn

## HdfsClient 常规操作
 + 新建文件
 + 查询文件信息
 + 重命名文件
 + 删除文件
 + 上传文件
 + 下载文件
 
## HDFS-HA工作机制
   **TODO**
   
# MapReduce学习
## WordCount入门程序

## Flow 流量统计程序

## 订单统计

## 自定义输出

## ETL数据清洗
+ 对Web访问日志中的各字段识别切分，去除日志中不合法的记录。根据清洗规则，输出过滤后的数据。
  
## hadoop数据压缩
+ Map输出端采用压缩
 Hadoop源码支持的压缩格式有：BZip2Codec 、DefaultCodec
```java
    Configuration configuration = new Configuration();
    // 开启map端输出压缩
    configuration.setBoolean("mapreduce.map.output.compress", true);
    // 设置map端输出压缩方式
    configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

```

+ Reduce输出端采用压缩
```java
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // 设置reduce端输出压缩开启
    FileOutputFormat.setCompressOutput(job, true);
    
    // 设置压缩的方式
    FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class); 
//  FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); 
//  FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class); 
```
