package com.cycloneboy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * created by sl on 19-3-2 - 下午7:52
 */
public class SimpleExample {

    public static void main(String[] args) throws URISyntaxException {

        // 获取配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        conf.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));

        // 创建连接
        try(Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin()){
            // 定义表名
            TableName tableName = TableName.valueOf("mytable");

//            // 定义表
//            HTable table = new HTable(tableName);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
