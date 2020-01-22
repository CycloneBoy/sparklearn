package com.cycloneboy.bigdata.stormlearn.common.log;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Create by sl on 2020-01-14 18:59 */
@Getter
@AllArgsConstructor
public enum UserType {

  // 1:学生 2:数码产品高频购买者 3:家庭主人 4: 白领 5: 吃货

  SUTDENT(1, "学生"),
  DIGITALMASTER(2, "数码达人"),
  HOMER(3, "家庭主人"),
  WORKER(4, "白领"),
  FOODER(5, "吃货");

  private int type;
  private String name;

  public static UserType valueOfType(int type) throws Exception {
    for (UserType user : UserType.values()) {
      if (user.getType() == type) {
        return user;
      }
    }

    throw new Exception("参数错误");
  }
}
