package com.cycloneboy.bigdata.communication.conf;

import com.cycloneboy.bigdata.communication.common.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

/** Create by sl on 2019-12-03 17:05 */
@Slf4j
public class ConfigurationManagerUtils {

  // FileBasedConfigurationBuilder:产生一个传入的类的实例对象
  // FileBasedConfiguration:融合FileBased与Configuration的接口
  // PropertiesConfiguration:从一个或者多个文件读取配置的标准配置加载器
  // configure():通过params实例初始化配置生成器
  // 向FileBasedConfigurationBuilder()中传入一个标准配置加载器类，生成一个加载器类的实例对象，然后通过params参数对其初始化

  public static Parameters params = new Parameters();
  // Read data from this file

  public static FileBasedConfigurationBuilder<FileBasedConfiguration> builder = null;

  public static Configuration config = null;

  static {
    builder =
        new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
            .configure(params.properties().setFileName("communication.properties"));

    try {
      config = builder.getConfiguration();
    } catch (ConfigurationException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    String startTime = ConfigurationManagerUtils.config.getString(Constants.CONF_START_DATE());

    log.info(startTime);
  }
}
