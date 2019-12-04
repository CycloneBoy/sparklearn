package com.cycloneboy.bigdata.communication.annocation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Create by sl on 2019-12-04 16:44 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TableRef {

  /**
   * 表名称
   *
   * @return
   */
  String value();
}
