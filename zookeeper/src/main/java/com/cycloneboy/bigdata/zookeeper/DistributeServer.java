package com.cycloneboy.bigdata.zookeeper;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/** Create by sl on 2019-10-29 21:10 */
@Slf4j
public class DistributeServer {

  private ZooKeeper zkCli;
  private static final String CONNECT_STRING = "localhost:2181";
  public static final int SESSION_TIMEOUT = 2000;

  private String parentNode = "/servers";

  /**
   * // 创建到zk的客户端连接
   *
   * @throws IOException
   */
  public void getConnect() throws IOException {
    zkCli =
        new ZooKeeper(
            CONNECT_STRING,
            SESSION_TIMEOUT,
            e -> {
              log.info("默认回调函数");
            });
  }

  /**
   * 注册服务器
   *
   * @param hostname
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void registerServer(String hostname) throws KeeperException, InterruptedException {
    String s =
        zkCli.create(
            parentNode + "/server",
            hostname.getBytes(),
            Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);

    log.info("{} is online create", hostname);
  }

  /**
   * 业务功能
   *
   * @param hostname
   * @throws InterruptedException
   */
  public void business(String hostname) throws InterruptedException {
    log.info("{} is working ...", hostname);

    Thread.sleep(Long.MAX_VALUE);
  }

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    DistributeServer server = new DistributeServer();
    server.getConnect();
    server.registerServer(args[0]);

    server.business(args[0]);
  }
}
