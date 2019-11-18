package com.cycloneboy.scala.standard.tempdata

/**
 *
 * Create by  sl on 2019-11-17 09:47
 */

case class TempData(day: Int, doy: Int, month: Int, year: Int,
                    precip: Double, snow: Double, tave: Double, tmax: Double, tmin: Double)


object TempData {

  def toDoubleOrNeg(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case _: NumberFormatException => -1
    }
  }

  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile("data/scala/MN212142_9392.csv")
    val lines = source.getLines().drop(1)
    val data = lines.flatMap { line =>
      val p = line.split(",")
      if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
        Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
          toDoubleOrNeg(p(5)), toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble,
          p(9).toDouble))

    }.toArray

    source.close()

    println(data.length)
    data.take(5) foreach println

    // 最热的一天
    println("-------------最热的一天--------------")
    val maxTemp = data.map(_.tmax).max
    val hotDays = data.filter(_.tmax == maxTemp)
    println(s"Hot days are ${hotDays.mkString(", ")}")

    val hotDay = data.maxBy(_.tmax)
    println(s"Hot days 1 is $hotDay")

    val hotDay2 = {
      data.reduceLeft((d1, d2) => if (d1.tmax >= d2.tmax) d1 else d2)
    }
    println(s"Hot days 2 is $hotDay2")

    // 下雨一天
    println("-------------下雨一天--------------")

    val rainyCount = data.count(_.precip >= 1.0)
    println(s"There are $rainyCount rainy days. There is ${rainyCount * 100.0 / data.length} percent.")

    val (rainySum, rainyCount2) = data.foldLeft(0.0 -> 0) { case ((sum, cnt), td) =>
      if (td.precip < 1.0) (sum, cnt) else (sum + td.tmax, cnt + 1)
    }
    println(s"Average Rainy temp is ${rainySum / rainyCount2}")

    val (rainySum3, rainyCount3) = data.par.aggregate(0.0 -> 0)({ case ((sum, cnt), td) =>
      if (td.precip < 1.0) (sum, cnt) else (sum + td.tmax, cnt + 1)
    }, { case ((s1, c1), (s2, c2)) =>
      (s1 + s2, c1 + c2)
    })
    println(s"Average Rainy temp is ${rainySum3 / rainyCount3}")


    val rainyTemps = data.flatMap(td => if (td.precip < 1.0) Seq.empty else Seq(td.tmax))
    println(s"Average Rainy temp is ${rainyTemps.sum / rainyTemps.length}")


    // 每个月的平均温度
    println("-------------每个月的平均温度--------------")

    val monthGroups = data.groupBy(_.month)
    val monthlyTemp = monthGroups.map { case (m, days) =>
      m -> days.foldLeft(0.0)((sum, td) => sum + td.tmax / days.length)
    }
    monthlyTemp.toSeq.sortBy(_._1) foreach println

  }
}
