package com.cycloneboy.bigdata.businessanalysis.analyse.model

/**
 *
 * Create by  sl on 2019-11-27 17:46
 */

/**
 *
 * @param taskid
 * @param area
 * @param areaLevel
 * @param productid
 * @param cityInfos
 * @param clickCount
 * @param productName
 * @param productStatus
 */
case class AreaTop3Product(taskid: String,
                           area: String,
                           areaLevel: String,
                           productid: Long,
                           cityInfos: String,
                           clickCount: Long,
                           productName: String,
                           productStatus: String)

/**
 * 城市商品点击
 *
 * @param city_id
 * @param click_product_id
 */
case class CityIdProduct(city_id: Long, click_product_id: Long)

/**
 * 城市信息
 *
 * @param city_id
 * @param city_name
 * @param area
 */
case class CityId2CityInfo(city_id: Long, city_name: String, area: String)