package com.cycloneboy.bigdata.businessanalysis.analyse.utils

import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.conf.ConfigurationManager
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.reflect.ClassTag

/**
 *
 * Create by  sl on 2019-11-27 00:09
 */
object DataUtils {

  /**
   * 将DataFrame插入到Hive表中
   *
   * @param spark     SparkSQL客户端
   * @param tableName 表名
   * @param dataDF    DataFrame
   */
  def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame) = {
    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  /**
   * 保存数据到数据库mysql
   *
   * @param rddDataWithCaseClass
   * @param tableName
   * @return
   */
  def saveRDD2Mysql[T: ClassTag](rddDataWithCaseClass: Dataset[T], tableName: String) = {
    rddDataWithCaseClass.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.CONF_JDBC_URL))
      .option("dbtable", tableName)
      .option("user", ConfigurationManager.config.getString(Constants.CONF_JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.CONF_JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }
}
