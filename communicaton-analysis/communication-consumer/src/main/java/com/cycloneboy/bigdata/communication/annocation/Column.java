package com.cycloneboy.bigdata.communication.annocation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Create by sl on 2019-12-04 16:58 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

  /**
   * 列族
   *
   * @return
   */
  String family() default "info";

  /**
   * 列名称 <br>
   * 默认不写的话就用字段名称
   *
   * @return
   */
  String column() default "";
}
