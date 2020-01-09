package com.cycloneboy.bigdata.stormlearn.common.algo;

import lombok.Getter;
import lombok.Setter;

/** Create by sl on 2020-01-09 15:03 */
@Getter
@Setter
public class Data implements Comparable<Data> {

  /** 每年获得的飞行常客里程数 */
  private double mile;
  /** 玩视频游戏所耗时间百分比 */
  private double time;
  /** 每周消费的冰淇淋公升数 */
  private double icecream;
  /** 1 代表不喜欢的人 2 代表魅力一般的人 3 代表极具魅力的人 */
  private int type;
  /** 两个数据距离 */
  private double distance;

  @Override
  public int compareTo(Data o) {
    if (this.distance < o.getDistance()) {
      return -1;
    } else if (this.distance > o.getDistance()) {
      return 1;
    }
    return 0;
  }
}
