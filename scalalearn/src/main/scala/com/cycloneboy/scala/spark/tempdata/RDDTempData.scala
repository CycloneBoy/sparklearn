package com.cycloneboy.scala.spark.tempdata

import com.cycloneboy.scala.standard.tempdata.TempData
import com.cycloneboy.scala.standard.tempdata.TempData.toDoubleOrNeg
import org.apache.spark.{SparkConf, SparkContext}


/**
 *
 * Create by  sl on 2019-11-18 18:08
 */
object RDDTempData {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("Temp data").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val lines = sc.textFile("hdfs://localhost:9000/user/sl/MN212142_9392.csv").filter(!_.contains("Day"))

    val data = lines.flatMap { line =>
      val p = line.split(",")
      if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
        Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
          toDoubleOrNeg(p(5)), toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble,
          p(9).toDouble))

    }.cache()


    println(data.count())
    data.take(5) foreach println


    // 最热的一天
    println("-------------最热的一天--------------")
    val maxTemp = data.map(_.tmax).max
    val hotDays = data.filter(_.tmax == maxTemp)
    println(s"Hot days are ${hotDays.collect().mkString(", ")}")

    val hotDay = data.max()(Ordering.by(_.tmax))
    println(s"Hot days 1 is $hotDay")

    val hotDay2 = {
      data.reduce((d1, d2) => if (d1.tmax >= d2.tmax) d1 else d2)
    }
    println(s"Hot days 2 is $hotDay2")


    // 下雨一天
    println("-------------下雨一天--------------")

    val rainyCount = data.filter(_.precip >= 1.0).count()
    println(s"There are $rainyCount rainy days. There is ${rainyCount * 100.0 / data.count()} percent.")

    //    val (rainySum, rainyCount2) = data.flatMap(0.0 -> 0) { case ((sum, cnt), td) =>
    //      if (td.precip < 1.0) (sum, cnt) else (sum + td.tmax, cnt + 1)
    //    }
    //    println(s"Average Rainy temp is ${rainySum / rainyCount2}")

    val (rainySum3, rainyCount3) = data.aggregate(0.0 -> 0)({ case ((sum, cnt), td) =>
      if (td.precip < 1.0) (sum, cnt) else (sum + td.tmax, cnt + 1)
    }, { case ((s1, c1), (s2, c2)) =>
      (s1 + s2, c1 + c2)
    })
    println(s"Average Rainy temp is ${rainySum3 / rainyCount3}")


    val rainyTemps = data.flatMap(td => if (td.precip < 1.0) Seq.empty else Seq(td.tmax))
    println(s"Average Rainy temp is ${rainyTemps.sum / rainyTemps.count()}")


    // 每个月的平均温度
    println("-------------每个月的平均温度--------------")

    val monthGroups = data.groupBy(_.month)
    val monthlyHighTemp = monthGroups.map { case (m, days) =>
      m -> days.foldLeft(0.0)((sum, td) => sum + td.tmax / days.size)
    }
    monthlyHighTemp.sortBy(_._1) foreach println
    // 按顺序打印
    println("按顺序打印")
    monthlyHighTemp.collect().sortBy(_._1) foreach println

    println("----------------------------")
    val monthlyLowTemp = monthGroups.map { case (m, days) =>
      m -> days.foldLeft(0.0)((sum, td) => sum + td.tmin / days.size)
    }

    monthlyLowTemp.collect().sortBy(_._1) foreach println

    // 每个月的温度 统计
    println("-------------每个月的温度 统计--------------")
    println("Stdev of highs: " + data.map(_.tmax).stdev())
    println("Stdev of lows: " + data.map(_.tmin).stdev())
    println("Stdev of averages: " + data.map(_.tave).stdev())

    val bins = (-20.0 to 107.0 by 1.0).toArray
    val counts = data.map(_.tmax).histogram(bins, true)
    counts foreach println

    // 每年的平均温度
    println("-------------每年的平均温度--------------")
    val keyedByYear = data.map(td => td.year -> td)
    val averageTempsByYear = keyedByYear.aggregateByKey(0.0 -> 0)({ case ((sum, cnt), td) =>
      (sum + td.tmax, cnt + 1)
    }, { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) })
    val averageByYearData = averageTempsByYear.collect().sortBy(_._1)

    averageByYearData foreach println
    println("------------------年均气温--------------------")
    averageByYearData.map { case (y, (s, c)) => (y, s / c) }.sortBy(_._1) foreach println
  }

  //  val plot = Plot.scatterPlots(Seq(
  //    (monthlyHighTemp.map(_._1).collect(), monthlyHighTemp.map(_._2).collect(), 0xffff0000, 5),
  //    (monthlyLowTemp.map(_._1).collect(), monthlyHighTemp.map(_._2).collect(), 0xff0000ff, 5)
  //  ), "Temp", "Month", "Temperature")
  //  FXRenderer(plot, 800, 600)

}
