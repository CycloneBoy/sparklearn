package com.cycloneboy.bigdata.communication.utils;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/** Create by sl on 2019-12-05 15:51 */
public class LRUCache extends LinkedHashMap<String, Integer> implements Serializable {

  private int maxElements;

  public LRUCache(int maxSize) {
    super(maxSize, 0.75F, true);
    this.maxElements = maxSize;
  }

  @Override
  protected boolean removeEldestEntry(Entry<String, Integer> eldest) {
    return (size() > this.maxElements);
  }
}
