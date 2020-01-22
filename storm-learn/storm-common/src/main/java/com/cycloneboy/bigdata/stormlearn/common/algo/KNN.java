package com.cycloneboy.bigdata.stormlearn.common.algo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * KNN算法属于监督学习算法，是一种用于分类的非常简单的算法。
 *
 * <p>简单的说，KNN算法采用测量不同特征值之间的距离方法进行分类
 *
 * <p>1）计算已知类别数据集中的点与当前点之间的距离
 *
 * <p>2）按照距离递增次序排序
 *
 * <p>3）选取与当前距离最小的k个点
 *
 * <p>4）确定前k个点所在类别的出现频率
 *
 * <p>5）返回前k个点出现频率最高的类别作为当前点的预测分类
 *
 * <p>Create by sl on 2020-01-09 15:05
 */
@Slf4j
public class KNN {

  private List<Data> dataset = null;

  public KNN(String fileName) throws IOException {
    dataset = initDataSet(fileName);
  }

  private List<Data> initDataSet(String fileName) throws IOException {
    List<Data> list = new ArrayList<>();

    BufferedReader bufferedReader =
        new BufferedReader(
            new InputStreamReader(KNN.class.getClassLoader().getResourceAsStream(fileName)));
    String line = null;
    while ((line = bufferedReader.readLine()) != null) {
      Data data = new Data();
      String[] s = line.split("\t");
      data.setMile(Double.parseDouble(s[0]));
      data.setTime(Double.parseDouble(s[1]));
      data.setIcecream(Double.parseDouble(s[2]));
      if (s[3].equals("largeDoses")) {
        data.setType(3);
      } else if (s[3].equals("smallDoses")) {
        data.setType(2);
      } else {
        data.setType(1);
      }
      list.add(data);
    }
    return list;
  }

  /**
   * 算法核心
   *
   * @param data
   * @param dataset
   * @param k
   */
  public int knn(Data data, List<Data> dataset, int k) {

    for (Data data2 : dataset) {
      double distance = calDistance(data, data2);
      data2.setDistance(distance);
    }
    // 对距离进行排序，倒序
    Collections.sort(dataset);
    // 从前k个样本中，找到出现频率最高的类别
    int type1 = 0, type2 = 0, type3 = 0;
    for (int i = 0; i < k; i++) {
      Data d = dataset.get(i);
      if (d.getType() == 1) {
        ++type1;
        continue;
      } else if (d.getType() == 2) {
        ++type2;
        continue;
      } else {
        ++type3;
      }
    }
    System.out.println(type1 + "========" + type2 + "=========" + type3);
    if (type1 > type2) {
      if (type1 > type3) {
        return 1;
      } else {
        return 3;
      }
    } else {
      if (type2 > type3) {
        return 2;
      } else {
        return 3;
      }
    }
  }

  /**
   * 计算两个样本点之间的距离
   *
   * @param data
   * @param data2
   * @return
   */
  private double calDistance(Data data, Data data2) {
    double sum =
        Math.pow((data.getMile() - data2.getMile()), 2)
            + Math.pow((data.getIcecream() - data2.getIcecream()), 2)
            + Math.pow((data.getTime() - data2.getTime()), 2);
    return Math.sqrt(sum);
  }

  /**
   * 将数据集归一化处理<br>
   * <br>
   * newValue = (oldValue - min) / (max - min)
   */
  private List<Data> autoNorm(List<Data> oldDataSet) {
    List<Data> newDataSet = new ArrayList<Data>();
    // find max and min
    Map<String, Double> map = findMaxAndMin(oldDataSet);
    for (Data data : oldDataSet) {
      data.setMile(calNewValue(data.getMile(), map.get("maxDistance"), map.get("minDistance")));
      data.setTime(calNewValue(data.getTime(), map.get("maxTime"), map.get("minTime")));
      data.setIcecream(
          calNewValue(data.getIcecream(), map.get("maxIcecream"), map.get("minIcecream")));
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
  private double calNewValue(double oldValue, double maxValue, double minValue) {
    return (double) (oldValue - minValue) / (maxValue - minValue);
  }

  /**
   * find the max and the min
   *
   * @return
   */
  private Map<String, Double> findMaxAndMin(List<Data> oldDataSet) {
    Map<String, Double> map = new HashMap<String, Double>();

    double maxDistance = Integer.MIN_VALUE;
    double minDistance = Integer.MAX_VALUE;
    double maxTime = Double.MIN_VALUE;
    double minTime = Double.MAX_VALUE;
    double maxIcecream = Double.MIN_VALUE;
    double minIcecream = Double.MAX_VALUE;

    for (Data data : oldDataSet) {
      if (data.getMile() > maxDistance) {
        maxDistance = data.getMile();
      }
      if (data.getMile() < minDistance) {
        minDistance = data.getMile();
      }
      if (data.getTime() > maxTime) {
        maxTime = data.getTime();
      }
      if (data.getTime() < minTime) {
        minTime = data.getTime();
      }
      if (data.getIcecream() > maxIcecream) {
        maxIcecream = data.getIcecream();
      }
      if (data.getIcecream() < minIcecream) {
        minIcecream = data.getIcecream();
      }
    }
    map.put("maxDistance", maxDistance);
    map.put("minDistance", minDistance);
    map.put("maxTime", maxTime);
    map.put("minTime", minTime);
    map.put("maxIcecream", maxIcecream);
    map.put("minIcecream", minIcecream);

    return map;
  }

  /** 将数据集以散点图呈现 */
  public void show() {
    //    new ScatterPlotChart().showChart(dataset);
  }

  /**
   * 取已有数据的10%作为测试数据，这里我们选取100个样本作为测试样本，其余作为训练样本
   *
   * @throws IOException
   */
  public void test() throws IOException {
    List<Data> testDataSet = initDataSet("test.txt");
    // 归一化数据
    List<Data> newTestDataSet = autoNorm(testDataSet);
    List<Data> newDataSet = autoNorm(dataset);
    int errorCount = 0;
    for (Data data : newTestDataSet) {
      int type = knn(data, newDataSet, 6);
      if (type != data.getType()) {
        ++errorCount;
      }
    }

    System.out.println("错误率：" + (double) errorCount / testDataSet.size() * 100 + "%");
  }

  public static void main(String[] args) throws IOException {
    KNN knn = new KNN("datingTestSet.txt");
    knn.test();
  }
}
