package com.cycloneboy.bigdata.stormlearn.common.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/** Create by sl on 2020-01-14 18:52 */
public class KnnAlgo {

  // 训练数据
  private List<ViewLog> dataset = null;

  public KnnAlgo(String fileName) throws IOException {
    dataset = KnnUtil.initDataSet(fileName);
  }

  /**
   * 算法核心
   *
   * @param data
   * @param dataset
   * @param k
   */
  public int knn(ViewLog data, List<ViewLog> dataset, int k) {

    for (ViewLog data2 : dataset) {
      double distance = calDistance(data, data2);
      data2.setDistance(distance);
    }
    // 对距离进行排序，倒序
    Collections.sort(dataset);
    // 从前k个样本中，找到出现频率最高的类别
    Map<Integer, Integer> numMap = new TreeMap<>();

    for (int i = 0; i < k; i++) {
      ViewLog d = dataset.get(i);
      numMap.merge(d.getType(), 1, Integer::sum);
    }

    // 获取最高类别
    int type = -1;
    int max = -1;
    for (Entry<Integer, Integer> entry : numMap.entrySet()) {
      if (entry.getValue() > max) {
        type = entry.getKey();
        max = entry.getValue();
      }
      //      System.out.println("key:" + entry.getKey() + " -> " + numMap.get(entry.getKey()) + " "
      // + entry.getValue());
    }

    //    System.out.println("最大种类: " + type + " " + max);
    return type;
  }

  /**
   * 计算两个样本点之间的距离
   *
   * @param data
   * @param data2
   * @return
   */
  private double calDistance(ViewLog data, ViewLog data2) {
    double sum =
        Math.pow((data.getClickId() - data2.getClickId()), 2)
            + Math.pow((data.getClickType() - data2.getClickType()), 2)
            + Math.pow((data.getOrderId() - data2.getOrderId()), 2)
            + Math.pow((data.getOrderType() - data2.getOrderType()), 2);
    return Math.sqrt(sum);
  }

  /**
   * 将数据集归一化处理<br>
   * <br>
   * newValue = (oldValue - min) / (max - min)
   */
  private List<ViewLog> autoNorm(List<ViewLog> oldDataSet) {
    List<ViewLog> newDataSet = new ArrayList<>();
    // find max and min
    Integer maxClickId =
        oldDataSet.stream()
            .filter(o -> o.getClickId() != 0)
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

    Integer maxClickType =
        oldDataSet.stream()
            .filter(o -> o.getClickType() != 0)
            .map(ViewLog::getClickType)
            .distinct()
            .max((v1, v2) -> (int) (v1 - v2))
            .get();

    Integer minClickType =
        oldDataSet.stream()
            .filter(o -> o.getClickType() >= 0)
            .map(ViewLog::getClickType)
            .distinct()
            .max((v1, v2) -> (int) (v2 - v1))
            .get();

    Integer maxOrderId =
        oldDataSet.stream()
            .filter(o -> o.getOrderId() != 0)
            .map(ViewLog::getOrderId)
            .distinct()
            .max((v1, v2) -> (int) (v1 - v2))
            .get();

    Integer minOrderId =
        oldDataSet.stream()
            .filter(o -> o.getOrderId() >= 0)
            .map(ViewLog::getOrderId)
            .distinct()
            .max((v1, v2) -> (int) (v2 - v1))
            .get();

    Integer maxOrderType =
        oldDataSet.stream()
            .filter(o -> o.getOrderType() != 0)
            .map(ViewLog::getOrderType)
            .distinct()
            .max((v1, v2) -> (int) (v1 - v2))
            .get();

    Integer minOrderType =
        oldDataSet.stream()
            .filter(o -> o.getOrderType() >= 0)
            .map(ViewLog::getOrderType)
            .distinct()
            .max((v1, v2) -> (int) (v2 - v1))
            .get();

    for (ViewLog data : oldDataSet) {
      data.setClickId(calNewValue(data.getClickId(), maxClickId, minClickId));
      data.setClickType(calNewValue(data.getClickType(), maxClickType, minClickType));
      data.setOrderId(calNewValue(data.getOrderId(), maxOrderId, minOrderId));
      data.setOrderType(calNewValue(data.getOrderType(), maxOrderType, minOrderType));

      newDataSet.add(data);
    }
    return newDataSet;
  }

  /**
   * @param oldValue
   * @param maxValue
   * @param minValue
   * @return newValue = (oldValue - min) / (max - min)
   */
  private int calNewValue(double oldValue, double maxValue, double minValue) {
    return (int) ((oldValue - minValue) / (maxValue - minValue));
  }

  /**
   * 取已有数据的10%作为测试数据，这里我们选取100个样本作为测试样本，其余作为训练样本
   *
   * @throws IOException
   */
  public void test(String testFileName) throws IOException {
    List<ViewLog> testDataSet = KnnUtil.initDataSet(testFileName);
    // 归一化数据
    List<ViewLog> newTestDataSet = autoNorm(testDataSet);
    List<ViewLog> newDataSet = autoNorm(dataset);
    int errorCount = 0;
    for (ViewLog data : newTestDataSet) {
      int type = knn(data, newDataSet, 5);
      if (type != data.getType()) {
        ++errorCount;
      }
    }

    System.out.println("错误率：" + (double) errorCount / testDataSet.size() * 100 + "%");
  }

  public int testKnn(String line, int k) {
    ViewLog viewLog = new ViewLog();
    String[] s = line.split("\t");
    viewLog.setClickId(Integer.parseInt(s[4]));
    viewLog.setClickType(Integer.parseInt(s[5]));
    viewLog.setOrderId(Integer.parseInt(s[6]));
    viewLog.setOrderType(Integer.parseInt(s[7]));

    List<ViewLog> testDataSet = new ArrayList<>();
    testDataSet.add(viewLog);

    // 归一化数据
    List<ViewLog> newTestDataSet = autoNorm(testDataSet);
    List<ViewLog> newDataSet = autoNorm(dataset);
    int errorCount = 0;
    int type = knn(newTestDataSet.get(0), newDataSet, k);

    System.out.println("类别:" + line + "  -> " + type);
    return type;
  }

  public static void main(String[] args) throws IOException {
    KnnAlgo knn = new KnnAlgo("viewlogTrain.txt");
    knn.test("viewLogTest.txt");
  }
}
