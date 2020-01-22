package com.cycloneboy.bigdata.stormlearn.common.log;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.Test;

/** Create by sl on 2020-01-14 19:37 */
public class KnnAlgoTest {

  @Test
  public void test1() {
    List<ViewLog> oldDataSet = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ViewLog viewLog = new ViewLog();
      viewLog.setClickId(i * 10);
      oldDataSet.add(viewLog);
    }

    Integer maxClickId =
        oldDataSet.stream()
            .filter(o -> o.getClickId() >= 0)
            .map(ViewLog::getClickId)
            .distinct()
            .max((v1, v2) -> (int) (v1 - v2))
            .get();

    Integer minClickId =
        oldDataSet.stream()
            .filter(o -> o.getClickId() >= 0)
            .map(ViewLog::getClickId)
            .distinct()
            .max((v1, v2) -> (int) (v2 - v1))
            .get();

    System.out.println("最大值:" + maxClickId + " 最小值:" + minClickId);
  }

  @Test
  public void testSort() {
    List<ViewLog> dataset = new ArrayList<>();
    for (int i = 0; i < 17; i++) {
      ViewLog viewLog = new ViewLog();
      viewLog.setType(i % 5);
      viewLog.setDistance(i * 10);
      dataset.add(viewLog);
    }

    // 对距离进行排序，倒序
    Collections.sort(dataset);
    dataset.forEach(System.out::println);

    Map<Integer, Integer> numMap = new TreeMap<>();
    List<Integer> sum2 = new ArrayList<>(5);
    for (Integer integer : sum2) {
      sum2.add(0);
    }

    for (int i = 0; i < 7; i++) {
      ViewLog d = dataset.get(i);
      numMap.merge(d.getType(), 1, Integer::sum);
    }

    int type = -1;
    int max = -1;
    for (Entry<Integer, Integer> entry : numMap.entrySet()) {
      if (entry.getValue() > max) {
        type = entry.getKey();
        max = entry.getValue();
      }
      System.out.println(
          "key:" + entry.getKey() + " -> " + numMap.get(entry.getKey()) + " " + entry.getValue());
    }

    System.out.println("最大种类: " + type + " " + max);
  }

  @Test
  public void testViewLog() {
    /**
     * 动作日期 用户ID SessionId 页面ID 点击产品ID 点击类别ID 订单产品ID 订单类别ID 类别 123 10001-100020 20001-20100 1-20
     * 1-100 1-20 1-100 1
     */
    List<ViewData> list = new ArrayList<>();

    long time = System.currentTimeMillis() - 100 * 24 * 3600 * 1000;
    int userId = 100001;
    String sessionId = UUID.randomUUID().toString().replace("-", "").substring(0, 20);
    int pageId = 3000001;

    // 1-5
    int clickId = 1000;
    int clickType = 1000;

    int orderId = 1000;

    int orderType = 1000;

    ViewData viewData = new ViewData();

    int genNumber = 1000;

    Random random = new Random();

    for (int i = 0; i < genNumber; i++) {
      ViewData log = new ViewData();
      log.setTime(time + i * 60 * 1000);
      log.setUserId(userId + i);

      sessionId = UUID.randomUUID().toString().replace("-", "").substring(0, 20);
      log.setSessionId(sessionId);
      log.setPageId(i * 100 / 7 * 3);
      log.setClickId(random.nextInt(clickId));
      log.setClickType(random.nextInt(clickType));
      log.setOrderId(random.nextInt(orderId));
      log.setOrderType(random.nextInt(orderType));

      double result = log.getClickId() + log.getClickType() + log.getOrderId() + log.getOrderType();

      log.setType(((int) (result) / 1000 + 1));

      list.add(log);
    }

    //    for (ViewData data : list) {
    //      System.out.println(data.toString());
    //    }

    String filePath =
        "/home/sl/workspace/java/a2019/sparklearn/storm-learn/storm-common/src/main/resources/viewlogTrain.txt";

    try {
      OutputStreamWriter outputStreamWriter =
          new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8");

      for (ViewData data : list) {
        outputStreamWriter.write(data.toString() + "\r\n");
        //        System.out.println(data.toString());
      }

      // 一定要手动flush才可以确保每条数据都写入到文件一次
      outputStreamWriter.flush();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
