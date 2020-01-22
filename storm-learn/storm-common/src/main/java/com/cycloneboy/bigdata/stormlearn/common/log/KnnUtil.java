package com.cycloneboy.bigdata.stormlearn.common.log;

import com.cycloneboy.bigdata.stormlearn.common.algo.KNN;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/** Create by sl on 2020-01-14 18:55 */
public class KnnUtil {

  public static List<ViewLog> initDataSet(String fileName) throws IOException {
    List<ViewLog> list = new ArrayList<>();

    BufferedReader bufferedReader =
        new BufferedReader(
            new InputStreamReader(KNN.class.getClassLoader().getResourceAsStream(fileName)));
    String line = null;
    while ((line = bufferedReader.readLine()) != null) {
      ViewLog data = new ViewLog();
      String[] s = line.split("\t");
      data.setClickId(Integer.parseInt(s[4]));
      data.setClickType(Integer.parseInt(s[5]));
      data.setOrderId(Integer.parseInt(s[6]));
      data.setOrderType(Integer.parseInt(s[7]));

      UserType userType = null;
      try {
        userType = UserType.valueOfType(Integer.parseInt(s[8]));
        data.setType(userType.getType());

        list.add(data);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return list;
  }
}
