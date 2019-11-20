package com.cycloneboy.scala.spark.sql

import org.apache.spark.sql.SparkSession

/**
 *
 * Create by  sl on 2019-11-20 15:45
 */

// 创建一个样例类

case class TbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable

case class TbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double) extends Serializable

case class TbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int) extends Serializable


object StockAction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("StockAction")
      .getOrCreate()

    // 使用隐式转化
    import spark.implicits._

    // 读取tbStock
    println("---------------读取tbStock----------------")
    val tbStockRdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/sl/spark/tbStock.txt").cache()
    val tbStockDS = tbStockRdd.map { lines =>
      val p = lines.split(",")
      TbStock(p(0), p(1), p(2))
    }.toDS().cache()

    tbStockDS.take(5) foreach println
    println()

    // 读取tbStockDetail
    println("---------------读取tbStockDetail----------------")
    val tbStockDetailRdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/sl/spark/tbStockDetail.txt").cache()
    val tbStockDetailDS = tbStockDetailRdd.map { lines =>
      val p = lines.split(",")
      TbStockDetail(p(0), p(1).toInt, p(2), p(3).toInt, p(4).toDouble, p(5).toDouble)
    }.toDS().cache()

    tbStockDetailDS.take(5) foreach println
    println()

    // 读取tbDate
    println("---------------读取tbDate----------------")
    val tbDateRdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/sl/spark/tbDate.txt").cache()
    val tbDateDS = tbDateRdd.map { lines =>
      val p = lines.split(",")
      TbDate(p(0), p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt, p(5).toInt, p(6).toInt, p(7).toInt, p(8).toInt, p(9).toInt)
    }.toDS().cache()

    tbDateDS.take(5) foreach println
    println()

    // 创建临时表
    tbStockDS.createOrReplaceTempView("tbStock")

    tbDateDS.createOrReplaceTempView("tbDate")

    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")

    // 任务一: 统计所有订单中每年的销售单数、销售总额
    println("-----------------------任务一: 统计所有订单中每年的销售单数、销售总额------------------------")
    val totalNumberOfOrder = spark.sql("select tD.theyear, count(distinct a.ordernumber),sum(b.amount) from tbStock a\n    join tbStockDetail b on a.ordernumber = b.ordernumber\n    join tbDate tD on a.dateid = tD.dateid\ngroup by tD.theyear\norder by tD.theyear")
    totalNumberOfOrder.show()

    /**
     * 任务一: 统计所有订单中每年的销售单数、销售总额
     * +-------+---------------------------+--------------------+
     * |theyear|count(DISTINCT ordernumber)|         sum(amount)|
     * +-------+---------------------------+--------------------+
     * |   2004|                       1094|   3268115.499199999|
     * |   2005|                       3828| 1.325756414999999E7|
     * |   2006|                       3772|1.3680982900000008E7|
     * |   2007|                       4885|1.6719354559999993E7|
     * |   2008|                       4861|1.4674295300000008E7|
     * |   2009|                       2619|   6323697.189999999|
     * |   2010|                         94|  210949.65999999997|
     * +-------+---------------------------+--------------------+
     */

    // 任务二: 统计每年最大金额订单的销售额
    println("-----------------------任务二: 统计每年最大金额订单的销售额------------------------")
    val theYearMaxOrderAmount = spark.sql("select theyear,max(c.SumOfAmount) as SumOfAmount from (select a.dateid,a.ordernumber,sum(tSD.amount) as SumOfAmount from tbStock a\n                                                                                   join tbStockDetail tSD on a.ordernumber = tSD.ordernumber\n             group by a.dateid,a.ordernumber\n             order by a.dateid,a.ordernumber) c\n    join tbDate d on c.dateid = d.dateid\ngroup by theyear\norder by theyear desc ")
    theYearMaxOrderAmount.show()

    /**
     * 统计每年最大金额订单的销售额
     * +-------+------------------+
     * |theyear|       SumOfAmount|
     * +-------+------------------+
     * |   2010|13065.280000000002|
     * |   2009|25813.200000000008|
     * |   2008|           55828.0|
     * |   2007|          159126.0|
     * |   2006|           36124.0|
     * |   2005|38186.399999999994|
     * |   2004| 23656.79999999997|
     * +-------+------------------+
     */

    // 任务三: 计算所有订单中每年最畅销货品
    println("-----------------------任务三: 计算所有订单中每年最畅销货品------------------------")
    println("第一步、求出每年每个货品的销售额")
    val everyYearTotalAmount = spark.sql("SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid").cache()
    everyYearTotalAmount.show()

    /**
     * 第一步、求出每年每个货品的销售额
     * +-------+--------------+------------------+
     * |theyear|        itemid|       SumOfAmount|
     * +-------+--------------+------------------+
     * |   2007|FS527258169701|21842.970000000005|
     * |   2007|QY527233200101|            7464.5|
     * |   2007|K9127105010202| 8190.360000000001|
     * |   2007|AK215371910101|24603.639999999992|
     * |   2007|QY126320610204|            3279.8|
     * |   2007|04126324013201|            2544.8|
     * |   2008|MY427400270202|           16608.2|
     * |   2008|30126307110607|            1220.0|
     * |   2008|AK216169120201|29144.199999999997|
     * |   2008|YL526228310106|           16073.1|
     * |   2008|E2628217050106|            8895.6|
     * |   2008|YL427437320101|12331.799999999997|
     * |   2008|04526230081702|             882.0|
     * |   2008|e2526248010501|              90.0|
     * |   2008|MH215303070101|            8827.0|
     * |   2008|01526242670201|             572.0|
     * |   2008|07627267880406|             336.0|
     * |   2009|57126134961201|             270.0|
     * |   2009|QY127170180102|            2259.0|
     * |   2009|CF126168059004|              50.0|
     * +-------+--------------+------------------+
     * only showing top 20 rows
     */
    println("第二步、在第一步的基础上，统计每年单个货品中的最大金额")

    val everyYearSingleOrderMaxAmount = spark.sql("SELECT d.theyear, MAX(d.SumOfAmount) AS MaxOfAmount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d GROUP BY d.theyear").cache()
    everyYearSingleOrderMaxAmount.show()

    /**
     * 第二步、在第一步的基础上，统计每年单个货品中的最大金额
     * +-------+------------------+
     * |theyear|       MaxOfAmount|
     * +-------+------------------+
     * |   2007|           70225.1|
     * |   2006|113720.60000000008|
     * |   2004| 53401.76000000003|
     * |   2009|           30029.2|
     * |   2005| 56627.32999999997|
     * |   2010|            4494.0|
     * |   2008| 98003.59999999996|
     * +-------+------------------+
     */

    println("第三步、用最大销售额和统计好的每个货品的销售额join，以及用年join，集合得到最畅销货品那一行信息")
    val result = spark.sql("SELECT DISTINCT e.theyear, e.itemid, f.maxofamount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) e JOIN (SELECT d.theyear, MAX(d.sumofamount) AS maxofamount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d GROUP BY d.theyear ) f ON e.theyear = f.theyear AND e.sumofamount = f.maxofamount ORDER BY e.theyear").cache()
    result.show()

    /**
     * 第三步、用最大销售额和统计好的每个货品的销售额join，以及用年join，集合得到最畅销货品那一行信息
     * +-------+--------------+------------------+
     * |theyear|        itemid|       maxofamount|
     * +-------+--------------+------------------+
     * |   2004|JY424420810101| 53401.76000000003|
     * |   2005|24124118880102| 56627.32999999997|
     * |   2006|JY425468460101|113720.60000000008|
     * |   2007|JY425468460101|           70225.1|
     * |   2008|E2628204040101| 98003.59999999996|
     * |   2009|YL327439080102|           30029.2|
     * |   2010|SQ429425090101|            4494.0|
     * +-------+--------------+------------------+
     */

  }
}
