package com.cycloneboy.bigdata.businessanalysis.commons.utils

import java.util.Date

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 *
 * Create by  sl on 2019-11-25 14:46
 */
object DateUtils {

  val TIME = "yyyy-MM-dd HH:mm:ss"
  val FORMAT_DATE_TIME_SECOND = "yyyyMMddHHmmss"

  val TIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd")
  val DATEKEY_FORMAT = DateTimeFormat.forPattern("yyyyMMdd")
  val DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmm")
  val DATE_TIME_SECOND_FORMAT = DateTimeFormat.forPattern("yyyyMMddHHmmss")

  /**
   * 判断一个时间是否在另一个时间之前
   *
   * @param time1 第一个时间
   * @param time2 第二个时间
   * @return 判断结果
   */
  def before(time1: String, time2: String): Boolean = {
    if (TIME_FORMAT.parseDateTime(time1).isBefore(TIME_FORMAT.parseDateTime(time2))) {
      return true
    }
    false
  }

  /**
   * 判断一个时间是否在另一个时间之后
   *
   * @param time1 第一个时间
   * @param time2 第二个时间
   * @return 判断结果
   */
  def after(time1: String, time2: String): Boolean = {
    if (TIME_FORMAT.parseDateTime(time1).isAfter(TIME_FORMAT.parseDateTime(time2))) {
      return true
    }
    false
  }

  /**
   * 计算时间差值（单位为秒）
   *
   * @param time1 时间1
   * @param time2 时间2
   * @return 差值
   */
  def minus(time1: String, time2: String): Int = {
    (TIME_FORMAT.parseDateTime(time1).getMillis - TIME_FORMAT.parseDateTime(time2).getMillis) / 1000 toInt
  }

  /**
   * 获取年月日和小时
   *
   * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
   * @return 结果（yyyy-MM-dd_HH）
   */
  def getDateHour(datetime: String): String = {
    val date = datetime.split(" ")(0)
    val hourMinuteSecond = datetime.split(" ")(1)
    val hour = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }

  /**
   * 获取当天日期（yyyy-MM-dd）
   *
   * @return 当天日期
   */
  def getTodayDate(): String = {
    DateTime.now().toString(DATE_FORMAT)
  }

  /**
   * 获取昨天的日期（yyyy-MM-dd）
   *
   * @return 昨天的日期
   */
  def getYesterdayDate(): String = {
    DateTime.now().minusDays(1).toString(DATE_FORMAT)
  }

  /**
   * 格式化日期（yyyy-MM-dd）
   *
   * @param date Date对象
   * @return 格式化后的日期
   */
  def formatDate(date: Date): String = {
    new DateTime(date).toString(DATE_FORMAT)
  }

  /**
   * 格式化时间（yyyy-MM-dd HH:mm:ss）
   *
   * @param date Date对象
   * @return 格式化后的时间
   */
  def formatTime(date: Date): String = {
    new DateTime(date).toString(TIME_FORMAT)
  }

  /**
   * 解析时间字符串
   *
   * @param time 时间字符串
   * @return Date
   */
  def parseTime(time: String): Date = {
    TIME_FORMAT.parseDateTime(time).toDate
  }

  /**
   * 格式化日期key
   *
   * @param date
   * @return
   */
  def formatDateKey(date: Date): String = {
    new DateTime(date).toString(DATEKEY_FORMAT)
  }

  /**
   * 格式化日期key
   *
   * @return
   */
  def parseDateKey(datekey: String): Date = {
    DATEKEY_FORMAT.parseDateTime(datekey).toDate
  }

  /**
   * 格式化时间，保留到分钟级别
   * yyyyMMddHHmm
   *
   * @param date
   * @return
   */
  def formatTimeMinute(date: Date): String = {
    new DateTime(date).toString(DATE_TIME_FORMAT)
  }

  /**
   * 格式化时间，自定义时间格式
   *
   * @param date   日期
   * @param format 时间格式
   * @return
   */
  def formatDateTime(date: Date, format: String): String = {
    val formatter = DateTimeFormat.forPattern(format)
    new DateTime(date).toString(formatter)
  }

  /**
   * 获取当前时间格式: yyyyMMddHHmmss
   *
   * @return
   */
  def getTodayStandard() = {
    formatDateTime(new Date(), FORMAT_DATE_TIME_SECOND)
  }
}
