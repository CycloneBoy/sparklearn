package com.cycloneboy.bigdata.stormlearn.common.algo;

import lombok.Getter;
import lombok.Setter;

/** Create by sl on 2020-01-09 15:09 */
@Getter
@Setter
public class XY implements Comparable<XY> {
  public double x, y, distance;
  public int type = 0;

  public XY(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public XY(double x, double y, int type) {
    this.x = x;
    this.y = y;
    this.type = type;
  }

  public int findType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  @Override
  public int compareTo(XY o) {
    if (this.distance < o.distance) {
      return -1;
    } else if (this.distance > o.distance) {
      return 1;
    }
    return 0;
  }
}
