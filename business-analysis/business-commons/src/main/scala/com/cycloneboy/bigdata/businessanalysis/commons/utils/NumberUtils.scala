package com.cycloneboy.bigdata.businessanalysis.commons.utils

/**
 *
 * Create by  sl on 2019-11-25 14:47
 */
object NumberUtils {

  /**
   * 格式化小数
   *
   * @param scale 四舍五入的位数
   * @return 格式化小数
   */
  def formatDouble(num: Double, scale: Int): Double = {
    val bd = BigDecimal(num)
    bd.setScale(scale, BigDecimal.RoundingMode.HALF_UP).doubleValue()
  }
}
