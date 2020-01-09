package com.cycloneboy.bigdata.stormlearn.common.algo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/** Create by sl on 2020-01-09 15:08 */
@Slf4j
public class KNN_Serial {

  // 计算欧氏距离
  private static double O_distance(double a1, double a2, double b1, double b2) {
    return Math.sqrt(Math.pow((a1 - b1), 2) + Math.pow(a2 - b2, 2));
  }

  // 读取文件训练集,格式：x,y,type.
  public static List<XY> readfile(String filename) throws IOException {
    List<XY> list = new ArrayList<XY>();
    BufferedReader bufferedReader = new BufferedReader(new FileReader(filename));
    String line = null;
    while ((line = bufferedReader.readLine()) != null) {
      String[] a = line.split(",");
      list.add(new XY(Double.parseDouble(a[1]), Double.parseDouble(a[2]), Integer.parseInt(a[0])));
    }
    return list;
  }
  // 输入一个x,y求距离
  public static List<XY> alldistance(double x, double y, List<XY> xy) {
    for (XY o : xy) {
      o.distance = O_distance(x, y, o.x, o.y);
    }
    return xy;
  }
  // 三个数求最大
  public static int threemax(int type1, int type2, int type3) {
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
  // KNN实现
  public int knn(double x, double y, int k) throws IOException {
    List<XY> list =
        readfile(
            "/home/sl/workspace/java/a2019/sparklearn/storm-learn/storm-common/src/main/resources/data.txt");
    List<XY> list1 = alldistance(x, y, list);
    Collections.sort(list1);
    int type1 = 0, type2 = 0, type3 = 0;
    for (int i = 0; i < k; k++) {
      XY o = list1.get(i);
      if (o.type == 1) {
        type1++;
      } else if (o.type == 2) {
        type2++;
      } else if (o.type == 3) {
        type3++;
      }
    }
    return threemax(type1, type2, type3);
  }

  public static void main(String[] args) throws IOException {
    KNN_Serial knn_serial = new KNN_Serial();
    int knn = knn_serial.knn(10.0, 2.2, 3);
    log.info("knn: {}", knn);
  }
}
