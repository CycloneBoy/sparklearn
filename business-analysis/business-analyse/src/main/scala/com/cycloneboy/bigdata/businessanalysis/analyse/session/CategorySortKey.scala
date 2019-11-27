package com.cycloneboy.bigdata.businessanalysis.analyse.session

/**
 *
 * Create by  sl on 2019-11-27 10:56
 *
 * 自定义类别排序
 */
case class CategorySortKey(clickCount: Long, orderCount: Long, payCount: Long)
  extends Ordered[CategorySortKey] {

  /** Result of comparing `this` with operand `that`.
   *
   * Implement this method to determine how instances of A will be sorted.
   *
   * Returns `x` where:
   *
   *   - `x < 0` when `this < that`
   *
   *   - `x == 0` when `this == that`
   *
   *   - `x > 0` when  `this > that`
   *
   */
  override def compare(that: CategorySortKey): Int = {
    if (this.clickCount - that.clickCount != 0) {
      return (this.clickCount - that.clickCount).toInt
    } else if (this.orderCount - that.orderCount != 0) {
      return (this.orderCount - that.orderCount).toInt
    } else if (this.payCount - that.payCount != 0) {
      return (this.payCount - that.payCount).toInt
    }

    0
  }

}
