package com.cycloneboy.bigdata.businessanalysis.analyse.product

import java.util.UUID

import com.cycloneboy.bigdata.businessanalysis.analyse.model.{CityId2CityInfo, CityIdProduct}
import com.cycloneboy.bigdata.businessanalysis.analyse.utils.ProcessUtils
import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.conf.ConfigurationManager
import com.cycloneboy.bigdata.businessanalysis.commons.utils.DateUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * Create by  sl on 2019-11-27 17:47
 */
object AreaTop3ProductApp {

  def main(args: Array[String]): Unit = {
    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)


    val taskUUID = DateUtils.getTodayStandard() + "_" + UUID.randomUUID().toString.replace("-", "")

    val sparkConf = new SparkConf().setAppName("AreaTop3Product").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("error")

    // 注册自定义函数
    spark.udf.register("concat_long_string", (v1: Long, v2: String, split: String) => v1.toString + split + v2)
    //    spark.udf.register("concat_long_string", (v1: Long, v2: String, split: String) => v1.toString + split + v2)

    spark.udf.register("get_json_object", (json: String, field: String) => {
      JSONObject.fromObject(json).getString(field)
    })

    spark.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF())


    val cityidProductRDD: RDD[CityIdProduct] = ProcessUtils.getCityId2ClickActionRDDByDateRange(spark, taskParam)

    // 查询用户指定日期范围内的点击行为数据（city_id，在哪个城市发生的点击行为）
    val cityId2ClickActionRDD: RDD[(Long, CityIdProduct)] = cityidProductRDD.map(item => (item.city_id, item))
    cityId2ClickActionRDD.cache()

    // 查询城市信息
    // 使用(city_id , 城市信息)
    val cityId2CityInfoRDD: RDD[(Long, CityId2CityInfo)] = ProcessUtils.getCityId2CityInfoRDD(spark)
    cityId2CityInfoRDD.cache()

    // 生成点击商品基础信息临时表
    // 将点击行为cityid2clickActionRDD和城市信息cityid2cityInfoRDD进行Join关联
    // tmp_click_product_basic
    generateTempClickProductBasicTable(spark, cityId2ClickActionRDD, cityId2CityInfoRDD)
    printTable(spark, "tmp_click_product_basic")

    spark.sql("select concat_long_string(100L,'city_name',':')").show()

    // 生成各区域各商品点击次数的临时表
    // 对tmp_click_product_basic表中的数据进行count聚合统计，得到点击次数
    // tmp_area_product_click_count
    generateTempAreaProductClickCountTable(spark)
    printTable(spark, "tmp_area_product_click_count")


    // 生成包含完整商品信息的各区域各商品点击次数的临时表
    // 关联tmp_area_product_click_count表与product_info表，在tmp_area_product_click_count基础上引入商品的详细信息
    //    generateTempAreaFullProductClickCountTable(spark)

    // 需求一：使用开窗函数获取各个区域内点击次数排名前3的热门商品
    //    val areaTop3ProductRDD: DataFrame = getAreaTop3ProductRDD(taskUUID, spark)
    //    ProcessUtils.printRDD(areaTop3ProductRDD.rdd)

    // 将数据转换为DF，并保存到MySQL数据库
    //    val areaTop3ProductDS: Dataset[AreaTop3Product] = areaTop3ProductRDD.rdd.map(row =>
    //      AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"), row.getAs[Long]("product_id"), row.getAs[String]("city_infos"), row.getAs[Long]("click_count"), row.getAs[String]("product_name"), row.getAs[String]("product_status"))
    //    ).toDS()
    //
    //    DataUtils.saveRDD2Mysql(areaTop3ProductDS, "area_top3_product")

    spark.close()
  }

  /**
   * 打印测试表格
   *
   * @param spark
   * @param tableName
   */
  def printTable(spark: SparkSession, tableName: String) = {
    println(s"------------------------BEGIN TABLE: $tableName ------------------------------------")
    val frame: DataFrame = spark.sql("select * from " + tableName)

    frame.take(5) foreach println
    println("---------------------------END---------------------------------")
  }

  /**
   * 需求一：获取各区域top3热门商品
   * 使用开窗函数先进行一个子查询,按照area进行分组，给每个分组内的数据，按照点击次数降序排序，打上一个组内的行号
   * 接着在外层查询中，过滤出各个组内的行号排名前3的数据
   *
   * @param taskUUID
   * @param spark
   */
  def getAreaTop3ProductRDD1(taskUUID: String, spark: SparkSession) = {
    // 华北、华东、华南、华中、西北、西南、东北
    // A级：华北、华东
    // B级：华南、华中
    // C级：西北、西南
    // D级：东北

    // case when
    // 根据多个条件，不同的条件对应不同的值
    // case when then ... when then ... else ... end
    val sql = "select " +
      "area, " +
      "case " +
      "WHEN area='China North' OR area='China East' THEN 'A Level' " +
      "WHEN area='China South' OR area='China Middle' THEN 'B Level' " +
      "WHEN area='West North' OR area='West South' THEN 'C Level' " +
      "ELSE 'D Level' " +
      "end area_level," +
      "product_id," +
      "city_infos," +
      "click_count," +
      "product_name," +
      "product_status " +
      "from (" +
      "select " +
      "area," +
      "product_id," +
      "click_count," +
      "city_infos," +
      "product_name," +
      "product_status," +
      "row_number() over (partition by area order by click_count desc) rank " +
      " from tmp_area_fullprod_click_count" +
      ")  t " +
      "where rank <= 3"

    spark.sql(sql)
  }

  /**
   * 需求一：获取各区域top3热门商品
   * 使用开窗函数先进行一个子查询,按照area进行分组，给每个分组内的数据，按照点击次数降序排序，打上一个组内的行号
   * 接着在外层查询中，过滤出各个组内的行号排名前3的数据
   *
   * @return
   */
  def getAreaTop3ProductRDD(taskid: String, spark: SparkSession): DataFrame = {

    // 华北、华东、华南、华中、西北、西南、东北
    // A级：华北、华东
    // B级：华南、华中
    // C级：西北、西南
    // D级：东北

    // case when
    // 根据多个条件，不同的条件对应不同的值
    // case when then ... when then ... else ... end

    val sql = "SELECT " +
      "area," +
      "CASE " +
      "WHEN area='China North' OR area='China East' THEN 'A Level' " +
      "WHEN area='China South' OR area='China Middle' THEN 'B Level' " +
      "WHEN area='West North' OR area='West South' THEN 'C Level' " +
      "ELSE 'D Level' " +
      "END area_level," +
      "product_id," +
      "city_infos," +
      "click_count," +
      "product_name," +
      "product_status " +
      "FROM (" +
      "SELECT " +
      "area," +
      "product_id," +
      "click_count," +
      "city_infos," +
      "product_name," +
      "product_status," +
      "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank " +
      "FROM tmp_area_fullprod_click_count " +
      ") t " +
      "WHERE rank<=3"

    spark.sql(sql)
  }

  /**
   * 生成区域商品点击次数临时表（包含了商品的完整信息）
   *
   * @param spark
   */
  def generateTempAreaFullProductClickCountTable(spark: SparkSession): Unit = {
    // 将之前得到的各区域各商品点击次数表，product_id
    // 去关联商品信息表，product_id，product_name和product_status
    // product_status要特殊处理，0，1，分别代表了自营和第三方的商品，放在了一个json串里面
    // get_json_object()函数，可以从json串中获取指定的字段的值
    // if()函数，判断，如果product_status是0，那么就是自营商品；如果是1，那么就是第三方商品
    // area, product_id, click_count, city_infos, product_name, product_status

    // 你拿到到了某个区域top3热门的商品，那么其实这个商品是自营的，还是第三方的
    // 其实是很重要的一件事

    // 技术点：内置if函数的使用
    val sql = "select " +
      "tapcc.area, " +
      "tapcc.product_id, " +
      "tapcc.click_count, " +
      "tapcc.city_infos, " +
      "pi.product_name, " +
      "if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status " +
      " from tmp_area_product_click_count tapcc " +
      " join product_info pi on tapcc.product_id=pi.product_id "

    val df = spark.sql(sql)
    df.createOrReplaceTempView("tmp_area_fullprod_click_count")
  }

  /**
   * 生成各区域各商品点击次数临时表
   *
   * @param spark
   */
  def generateTempAreaProductClickCountTable(spark: SparkSession) = {
    // 按照area和product_id两个字段进行分组
    // 计算出各区域各商品的点击次数
    // 可以获取到每个area下的每个product_id的城市信息拼接起来的串
    //"city_id", "city_name", "area", "product_id"
    val sql1 = "select area,product_id,count(*) click_count," +
      "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
      "from tmp_click_product_basic " +
      "group by area,product_id"

    val sql2 = "SELECT " +
      //      "area," +
      "city_id," +
      "city_name," +
      "product_id," +
      "count(*) click_count, " +
      //      "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
      "FROM tmp_click_product_basic "

    val sql = "select city_id, city_name, area, product_id, " +
      "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
      " from tmp_click_product_basic "

    val df = spark.sql(sql)

    // 各区域各商品的点击次数（以及额外的城市列表）,再次将查询出来的数据注册为一个临时表
    df.createOrReplaceTempView("tmp_area_product_click_count")
  }

  /**
   * 生成点击商品基础信息临时表
   * 注册临时表 : tmp_click_product_basic("city_id","city_name","area","product_id")
   *
   * @param spark
   * @param cityId2ClickActionRDD
   * @param cityId2CityInfoRDD
   */
  def generateTempClickProductBasicTable(spark: SparkSession, cityId2ClickActionRDD: RDD[(Long, CityIdProduct)], cityId2CityInfoRDD: RDD[(Long, CityId2CityInfo)]) = {
    // 执行join操作，进行点击行为数据和城市数据的关联
    val joinedRDD: RDD[(Long, (CityIdProduct, CityId2CityInfo))] = cityId2ClickActionRDD.join(cityId2CityInfoRDD)

    // 将上面的JavaPairRDD，转换成一个JavaRDD<Row>（才能将RDD转换为DataFrame）
    val mappedRDD: RDD[(Long, String, String, Long)] = joinedRDD.map { case (cityid, (action, cityInfo)) =>
      (cityid, cityInfo.city_name, cityInfo.area, action.click_product_id)
    }

    // 1 北京
    // 2 上海
    // 1 北京
    // group by area,product_id
    // 1:北京,2:上海

    // 两个函数
    // UDF：concat2()，将两个字段拼接起来，用指定的分隔符
    // UDAF：group_concat_distinct()，将一个分组中的多个字段值，用逗号拼接起来，同时进行去重
    import spark.implicits._
    val df: DataFrame = mappedRDD.toDF("city_id", "city_name", "area", "product_id")

    df.createOrReplaceTempView("tmp_click_product_basic")
  }
}

















