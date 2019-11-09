package com.cycloneboy.bigdata.hbaselearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/** created by sl on 19-3-2 - 下午7:52 */
public class SimpleExample {

  public static void create() throws Exception {
    // 创建配置文件
    Configuration configuration = HBaseConfiguration.create();
    // 设置配置文件信息
    configuration.set("hbase.zookeeper.quorum", "localhost:2181");
    // 创建数据库对象
    HBaseAdmin admin = new HBaseAdmin(configuration);
    // 创建表对象
    HTableDescriptor hd = new HTableDescriptor(TableName.valueOf("tab02".getBytes()));
    // 创建列族对象
    HColumnDescriptor hc1 = new HColumnDescriptor("cf01".getBytes());
    // 设置列族保存最大历史版本
    hc1.setMaxVersions(3);
    HColumnDescriptor hc2 = new HColumnDescriptor("cf02".getBytes());
    hc2.setMaxVersions(3);
    hd.addFamily(hc1);
    hd.addFamily(hc2);
    // 创建表
    admin.createTable(hd);
    // 关闭连接
    admin.close();
  }

  public static void main(String[] args) throws Exception {
    SimpleExample.create();
  }
}
