[TOC]

# bigdata 学习(scala|spark|hbase)
## spark 
   + MySpark  测试spark是否可用,集合求和
   + WordCount 统计文件单词数量
   + PageRank  PageRank 算法
   + DomainNamePartitioner 自定义的域名分区器
   + LearnRdd PersistRdd Rdd 学习

## scalaleaen scala学习
   + HelloWorld  scala hello word,测试scala环境可用
   + myFirstScala scala hello word
   + 继续学习scala
   
### scala基础语法学习
  1. 函数,高阶函数,函数闭包,函数科里化,部分应用函数,偏函数
  2. 模式匹配和隐式转换
  3. 类型参数和高级类型
  ```text

      类型界定  T <: R  限制T的最顶层类R,上界R
      类型界定  T >: R  限制T必须是R的超类,下界R
      视图定界 S <% Comparable[S]
      类型变量界定要求类在类继承层次结构上
      视图界定不但可以在类继承层次结构上,还可以跨越类继承层次结构
  ```
  4. akka编程 基础
  ```text
    actor
    Typed Actor
    dispatcher 调度 -BalancingDispatcher 按照消息的优先级进行处理,高优先级的先处理
    Router
    容错
   ```
## spark 学习
### spark-core

### spark-sql

### spark-streaming

### spark实战-电商分析平台

### spark实战-实时项目

### spark-mllib

### spark性能调优


## spark-python 学习
   + [厦门租房信息分析展示（pycharm+python爬虫+pyspark+pyecharts）](http://dblab.xmu.edu.cn/blog/2307/)
   见 [spark-python/spark-learn/fishrent/run.py](https://github.com/CycloneBoy/sparklearn/blob/master/spark-python/spark-learn/fishrent/run.py)
   + WordCount 统计文件单词数量 见 [spark-python/spark-learn/demo/WordCount.py](https://github.com/CycloneBoy/sparklearn/blob/master/spark-python/spark-learn/demo/WordCount.py)`
   
## hbaselearn HBase学习
    + CRUD JAVA API 例子
   
## hadoop hadoop的学习
+ 添加hadoop/ hdfs的读写
+ 添加mapreduce 学习
+ 流量统计
+ 分区排序

###  ETL数据清洗
+ 去除日志中字段长度小于等于11的日志
+ 对Web访问日志中的各字段识别切分，去除日志中不合法的记录。根据清洗规则，输出过滤后的数据。
+ 对Hive谷粒影音实战项目进行数据清洗(VideoETL)
 
### hadoop数据压缩
+ Map输出端采用压缩
+ Reduce输出端采用压缩    
   
### MapReduce总结练习
1. 文件的倒排索引
2. topn 获取流量前十的用户信息
3. 找博客共同好友

## zookeeper学习
1. 基本命令行使用
2. 基本api使用
3. 原理学习

## Hive学习
1. 基本的sql学习
2. 自定义函数(UDF)
3. 数据存储和压缩
4. Hive实战之谷粒影音

```text
需求描述
统计硅谷影音视频网站的常规指标，各种TopN指标：
--统计视频观看数Top10
--统计视频类别热度Top10
--统计视频观看数Top20所属类别
--统计视频观看数Top50所关联视频的所属类别Rank
--统计每个类别中的视频热度Top10
--统计每个类别中视频流量Top10
--统计上传视频最多的用户Top10以及他们上传的视频
--统计每个类别视频观看数Top10
```
多重复合查询

5.TODO:  Hive Java api 学习

## flume学习
1. 自定义source
2. 自定义sink
3. 自定义source ,实时监控MySQL，从MySQL中获取数据传输到HDFS或者其他存储框架
4. flume的配置文件实例

## flink学习

## storm学习

## kafka学习

## elasticsearch学习

## ELK学习

## 数据挖掘和机器学习



## Issue 意见、建议

如有勘误、意见或建议欢迎拍砖 <https://github.com/CycloneBoy/sparklearn/issues>

## 联系作者:

您也可以直接联系我：

* 开源：https://github.com/CycloneBoy