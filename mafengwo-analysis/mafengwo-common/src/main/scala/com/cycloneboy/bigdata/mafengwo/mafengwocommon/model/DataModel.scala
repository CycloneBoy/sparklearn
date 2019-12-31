package com.cycloneboy.bigdata.mafengwo.mafengwocommon.model

import java.sql.Date

/**
 * 实体类
 * Create by  sl on 2019-12-31 21:21
 */

/**
 * 蜂首游记
 *
 * @param id               ID
 * @param year             游记年
 * @param month            游记月
 * @param day              游记日
 * @param url              游记链接
 * @param note_image_url   游记封面链接
 * @param destination      游记目的地
 * @param author_url       作者链接
 * @param author_name      作者名称
 * @param author_image_url 游记作者图片链接
 * @param create_time      创建时间
 * @param note_type        游记类型
 */
case class TravelNote(id: Integer,
                      year: Integer,
                      month: Integer,
                      day: Integer,
                      url: String,
                      note_image_url: String,
                      destination: String,
                      author_url: String,
                      author_name: String,
                      author_image_url: String,
                      create_time: Date,
                      note_type: Integer
                     )

/**
 * 马蜂窝热门景点的热门游记列表汇总
 *
 * @param id                 ID
 * @param country_name       热门目的地国家名称
 * @param country_url        热门目的地国家链接
 * @param city_name          热门目的地城市名称
 * @param city_url           热门目的地城市链接
 * @param city_index         热门目的地城市的编码
 * @param total_page         游记总页数
 * @param total_number       游记总数
 * @param image_total_number 游记图片总数
 * @param crawl_status       是否已经爬取
 * @param crawl_time         爬取时间
 * @param create_datetime    创建时间
 * @param city_name_en       热门目的地城市英文名称
 */
case class TravelHotNote(id: Long,
                         country_name: String,
                         country_url: String,
                         city_name: String,
                         city_url: String,
                         city_index: String,
                         total_page: Long,
                         total_number: Long,
                         image_total_number: String,
                         crawl_status: String,
                         crawl_time: String,
                         create_datetime: Date,
                         city_name_en: String
                        )

/**
 * 马蜂窝热门游记列表 马蜂窝热门景点的热门游记列表
 *
 * @param id                         ID
 * @param resource_type              游记爬取来源
 * @param travel_url                 游记链接
 * @param travel_name                游记名称
 * @param travel_type                游记分类 ： 宝藏 、 星级'
 * @param travel_summary             游记摘要
 * @param travel_destination         游记目的地
 * @param travel_destination_country 游记目的地国家
 * @param travel_image_url           游记封面图片链接
 * @param author_id                  游记作者ID
 * @param author_url                 游记作者首页链接
 * @param author_name                游记作者名称
 * @param author_image_url           游记作者图像链接
 * @param travel_view_count          游记浏览总数
 * @param travel_comment_count       游记评论总数
 * @param travel_up_count            游记顶的总数
 * @param travel_father_id           游记父亲ID
 * @param travel_id                  游记ID
 * @param crawl_status               是否已经爬取
 * @param crawl_time                 爬取时间
 * @param create_datetime            创建时间
 */
case class TravelHotNoteDetail(
                                id: Long,
                                resource_type: String,
                                travel_url: String,
                                travel_name: String,
                                travel_type: String,
                                travel_summary: String,
                                travel_destination: String,
                                travel_destination_country: String,
                                travel_image_url: String,
                                author_id: String,
                                author_url: String,
                                author_name: String,
                                author_image_url: String,
                                travel_view_count: Long,
                                travel_comment_count: Long,
                                travel_up_count: Long,
                                travel_father_id: String,
                                travel_id: String,
                                crawl_status: String,
                                crawl_time: String,
                                create_datetime: Date
                              )

