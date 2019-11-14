package com.cycloneboy.scala.standard.ch2;

import scala.collection.mutable.HashMap;
import scala.collection.mutable.Map;

/** Create by sl on 2019-11-14 10:49 */
public class ScalaCollectionInJava {

  public static void main(String[] args) {
    Map<String, String> scalaBigDataTools = new HashMap<>();
    scalaBigDataTools.put("Hadoop", "the most popular big data tools");
    scalaBigDataTools.put("hive", "the most popular interative query tools");

    // 以下代码是OK的
    //    java.util.Map<String, String> javaBigDataTools =
    // JavaConverters.mapAsJavaMap(scalaBigDataTools);
    //
    //    for (String key : javaBigDataTools.keySet()) {
    //      System.out.println(javaBigDataTools.get(key));
    //    }
  }
}
