package com.cycloneboy.hbase.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MainTest {

//    @Test
//    public void create() throws Exception {
//        //创建配置文件
//        Configuration configuration= HBaseConfiguration.create();
//        //设置配置文件信息
//        configuration.set("hbase.zookeeper.quorum", "yun01:2181,yun02:2181,yun03:2181");
//        //创建数据库对象
//        HBaseAdmin admin=new HBaseAdmin(configuration);
//        //创建表对象
//        HTableDescriptor hd=new HTableDescriptor(TableName.valueOf("tab02".getBytes()));
//        //创建列族对象
//        HColumnDescriptor hc1=new HColumnDescriptor("cf01".getBytes());
//        //设置列族保存最大历史版本
//        hc1.setMaxVersions(3);
//        HColumnDescriptor hc2=new HColumnDescriptor("cf02".getBytes());
//        hc2.setMaxVersions(3);
//        hd.addFamily(hc1);
//        hd.addFamily(hc2);
//        //创建表
//        admin.createTable(hd);
//        //关闭连接
//        admin.close();
//    }
//
//    @Test
//    public void Insert() throws Exception {
//        // 创建配置文件
//        Configuration conf = HBaseConfiguration.create();
//        // 设置配置文件信息
//        conf.set("hbase.zookeeper.quorum", "yun01:2181,yun02:2181,yun03:2181");
//        // 创建表对象
//        HTable table = new HTable(conf, "tab02".getBytes());
//        // 创建添加数据的行键对象
//        Put put = new Put("row01".getBytes());
//        // 设置行键数据
//        put.add("cf01".getBytes(), "c01".getBytes(), "wz001".getBytes());
//        put.add("cf02".getBytes(), "c01".getBytes(), "wz0010".getBytes());
//        put.add("cf01".getBytes(), "c02".getBytes(), "wz010".getBytes());
//        // 添加数据
//        table.put(put);
//        // 关闭连接
//        table.close();
//    }
//
//    @Test
//    public void insertMach() throws Exception{
//        Configuration conf=HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "yun01:2181,yun03:2181,yun02:2181,");
//        HTable table = new HTable(conf, "tab02".getBytes());
//        List<Put> pList=new ArrayList<Put>();
//        for (int i = 1; i <= 1000000; i++) {
//            Put put = new Put(Bytes.toBytes("row"+i));
//            put.add("cf01".getBytes(), Bytes.toBytes("c"), Bytes.toBytes("val"+i));
//            pList.add(put);
//            if(i%10000==0) {
//                table.put(pList);
//                pList=new ArrayList<Put>();
//            }
//        }
//        table.put(pList);
//        table.close();
//    }
//
//    @Test
//    public void select() throws Exception {
//        // 创建配置文件对象
//        Configuration conf = HBaseConfiguration.create();
//        // 设置配置文件参数
//        conf.set("hbase.zookeeper.quorum", "yun01:2181,yun02:2181,yun03:2181,");
//        // 创建表对象
//        HTable table = new HTable(conf, "tab02".getBytes());
//        // 创建获取数据的行键对象
//        Get get = new Get("row01".getBytes());
//        // 获取整行数据
//        Result result = table.get(get);
//        // 获取行数据值
//        byte[] bs = result.getValue("cf01".getBytes(), "c01".getBytes());
//        // 转化数据
//        String string = new String(bs);
//        System.err.println(string);
//        // 关闭连接
//        table.close();
//    }
//
//    @Test
//    public void resultList() throws Exception{
//        // 创建配置文件对象并配置参数
//        Configuration conf=HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "yun01:2181,yun02:2181,yun03:2181");
//        // 创建表对象
//        HTable table = new HTable(conf, "tab02".getBytes());
//        // 创建获取数据的行键对象
//        Scan scan = new Scan("row01".getBytes());
//        // 获取结果集
//        ResultScanner scanner = table.getScanner(scan);
//        // 遍历结果集
//        Iterator<Result> it = scanner.iterator();
//        while(it.hasNext()) {
//            Result result = it.next();
//            byte[] bs = result.getValue("cf01".getBytes(), "c01".getBytes());
//            String str=new String(bs);
//            System.err.println(str);
//        }
//        // 关闭连接
//        table.close();
//    }
//
//    @Test
//    public void delete() throws Exception{
//        //创建配置文件对象并配置参数
//        Configuration conf=HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "yun01:2181,yun02:2181,yun03:2181");
//        // 创建表对象
//        HTable table = new HTable(conf, "tab02".getBytes());
//        // 创建删除对象
//        Delete delete = new Delete("row1".getBytes());
//        // 删除数据
//        table.delete(delete);
//        // 关闭连接
//        table.close();
//    }
//
//    @Test
//    public void deleteTable() throws Exception{
//        // 创建配置文件对象并配置参数
//        Configuration conf= HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "yun01:2181,yun02:2181,yun03:2181");
//        // 创建数据库对象
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        // 禁用表
//        admin.disableTable("tab01".getBytes());
//        // 删除表
//        admin.deleteTable("tab01".getBytes());
//        // 关闭连接
//        admin.close();
//    }
}