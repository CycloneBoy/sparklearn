package com.cycloneboy.bigdata.mafengwo.mafengwocommon.conf


import com.cycloneboy.bigdata.mafengwo.mafengwocommon.common.Constants
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}

/**
 *
 * Create by  sl on 2019-11-25 13:56
 *
 * 配置工具类
 */
object ConfigurationManager {

  private val params = new Parameters()

  // FileBasedConfigurationBuilder:产生一个传入的类的实例对象
  // FileBasedConfiguration:融合FileBased与Configuration的接口
  // PropertiesConfiguration:从一个或者多个文件读取配置的标准配置加载器
  // configure():通过params实例初始化配置生成器
  // 向FileBasedConfigurationBuilder()中传入一个标准配置加载器类，生成一个加载器类的实例对象，然后通过params参数对其初始化
  private val builder = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
    .configure(params.properties().setFileName("mafengwo.properties"))

  val config = builder.getConfiguration


  def main(args: Array[String]): Unit = {
    print(ConfigurationManager.config.getString(Constants.CONF_JDBC_URL))
  }
}
