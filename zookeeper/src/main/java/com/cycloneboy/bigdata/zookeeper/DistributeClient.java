package com.cycloneboy.bigdata.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/** Create by sl on 2019-10-29 21:32 */
@Slf4j
public class DistributeClient {

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
              try {
                getServerList();
              } catch (KeeperException ex) {
                ex.printStackTrace();
              } catch (InterruptedException ex) {
                ex.printStackTrace();
              }
              log.info("默认回调函数");
            });
  }

  /**
   * 获取服务列表
   *
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void getServerList() throws KeeperException, InterruptedException {

    // 1获取服务器子节点信息，并且对父节点进行监听
    List<String> children = zkCli.getChildren(parentNode, true);

    // 2存储服务器信息列表
    ArrayList<String> servers = new ArrayList<>();

    // 3遍历所有节点，获取节点中的主机名称信息
    for (String child : children) {
      byte[] data = zkCli.getData(parentNode + "/" + child, false, null);
      servers.add(new String(data));
    }

    // 4打印服务器列表信息
    log.info("Servers list :{}", servers);
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
    DistributeClient client = new DistributeClient();
    client.getConnect();
    client.getServerList();
    client.business(args[0]);
  }
}
