package com.cycloneboy.bigdata.zookeeper;

import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

/** Create by sl on 2019-10-29 20:14 */
@Slf4j
public class ZkClient {

  private ZooKeeper zkCli;
  private static final String CONNECT_STRING = "localhost:2181";
  public static final int SESSION_TIMEOUT = 2000;

  @Before
  public void before() throws IOException {
    zkCli =
        new ZooKeeper(
            CONNECT_STRING,
            SESSION_TIMEOUT,
            e -> {
              log.info("默认回调函数");
            });
  }

  @Test
  public void ls() throws KeeperException, InterruptedException {
    List<String> children =
        zkCli.getChildren(
            "/",
            e -> {
              log.info("自定义回调函数");
            });

    log.info("===================================================");

    for (String child : children) {
      log.info(child);
    }

    log.info("===================================================");

    Thread.sleep(Long.MAX_VALUE);
  }

  @Test
  public void create() throws KeeperException, InterruptedException {
    String s =
        zkCli.create("/Idea", "Idea2019".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

    log.info(s);

    Thread.sleep(Long.MAX_VALUE);
  }

  @Test
  public void get() throws KeeperException, InterruptedException {
    byte[] data = zkCli.getData("/ideatest0000000017", true, new Stat());

    String string = new String(data);

    log.info(string);
  }

  @Test
  public void set() throws KeeperException, InterruptedException {
    Stat stat = zkCli.setData("/ideatest0000000017", "adbcedf".getBytes(), 0);

    log.info(stat.toString());
  }

  @Test
  public void stat() throws KeeperException, InterruptedException {
    Stat exists = zkCli.exists("/Idea", false);
    if (exists == null) {
      log.info("节点不存在");
    } else {
      log.info(String.valueOf(exists.getDataLength()));
    }
  }

  @Test
  public void delete() throws KeeperException, InterruptedException {
    Stat exists = zkCli.exists("/ideatest0000000017", false);
    if (exists == null) {
      zkCli.delete("/ideatest0000000017", exists.getVersion());
    }
  }

  public void register() throws KeeperException, InterruptedException {
    byte[] data =
        zkCli.getData(
            "/a",
            new Watcher() {
              @Override
              public void process(WatchedEvent watchedEvent) {
                try {
                  register();
                } catch (KeeperException e) {
                  e.printStackTrace();
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
            },
            null);

    log.info(new String(data));
  }

  @Test
  public void testRegister() throws InterruptedException {
    try {
      register();
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Thread.sleep(Long.MAX_VALUE);
  }
}
