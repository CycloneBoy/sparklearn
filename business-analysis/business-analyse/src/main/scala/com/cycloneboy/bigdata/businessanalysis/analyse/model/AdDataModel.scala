package com.cycloneboy.bigdata.businessanalysis.analyse.model

/**
 *
 * Create by  sl on 2019-11-28 14:16
 */

case class AdClickInfo(long: Long,
                       date: String,
                       userid: Long,
                       adid: Long,
                       province: Long,
                       cityid: Long)

/**
 * 广告黑名单
 *
 * @author sl
 *
 */
case class AdBlacklist(userid: Long)

/**
 * 用户广告点击量
 *
 * @author sl
 *
 */
case class AdUserClickCount(date: String,
                            userid: Long,
                            adid: Long,
                            clickCount: Long)


/**
 * 广告实时统计
 *
 * @author sl
 *
 */
case class AdStat(date: String,
                  province: String,
                  city: String,
                  adid: Long,
                  clickCount: Long)

/**
 * 各省top3热门广告
 *
 * @author sl
 *
 */
case class AdProvinceTop3(date: String,
                          province: String,
                          adid: Long,
                          clickCount: Long)

/**
 * 广告点击趋势
 *
 * @author sl
 *
 */
case class AdClickTrend(date: String,
                        hour: String,
                        minute: String,
                        adid: Long,
                        clickCount: Long)