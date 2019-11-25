package com.cycloneboy.bigdata.businessanalysis.commons.pool

import java.sql.Connection

import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.conf.ConfigurationManager
import org.apache.commons.pool2.impl._
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

/**
 *
 * Create by  sl on 2019-11-25 12:41
 *
 * 创建自定义工厂类，继承 BasePooledObjectFactory 工厂类，负责对象的创建、包装和销毁
 *
 * @param jdbcUrl
 * @param jdbcUser
 * @param jdbcPassword
 * @param client
 */
class PooledMySqlClientFactory(jdbcUrl: String, jdbcUser: String, jdbcPassword: String,
                               client: Option[Connection] = None) extends BasePooledObjectFactory[MySqlProxy] with
  Serializable {
  // 用于池来创建对象
  override def create(): MySqlProxy = MySqlProxy(jdbcUrl, jdbcUser, jdbcPassword, client)

  // 用于池来包装对象
  override def wrap(obj: MySqlProxy): PooledObject[MySqlProxy] = new
      DefaultPooledObject(obj)

  // 用于池来销毁对象
  override def destroyObject(p: PooledObject[MySqlProxy]): Unit = {
    p.getObject.shutdown()
    super.destroyObject(p)
  }
}

/**
 * 创建 MySQL 池工具类
 */
object CreateMySqlPool {
  // 加载 JDBC 驱动，只需要一次
  val jdbcDriver = ConfigurationManager.config.getString(Constants.CONF_JDBC_DRIVER)

  Class.forName("com.mysql.cj.jdbc.Driver")
  // 在 org.apache.commons.pool2.impl 中预设了三个可以直接使用的对象池：GenericObjectPool、
  // GenericKeyedObjectPool 和 SoftReferenceObjectPool
  // 创建 genericObjectPool 为 GenericObjectPool
  // GenericObjectPool 的特点是可以设置对象池中的对象特征，包括 LIFO 方式、最大空闲数、最小空闲数、是否有效性检查等等
  private var genericObjectPool: GenericObjectPool[MySqlProxy] = null

  // 伴生对象通过 apply 完成对象的创建
  def apply(): GenericObjectPool[MySqlProxy] = {

    // 单例模式
    if (this.genericObjectPool == null) {
      this.synchronized {
        // 获取 MySQL 配置参数
        val jdbcUrl = ConfigurationManager.config.getString(Constants.CONF_JDBC_URL)
        val jdbcUser = ConfigurationManager.config.getString(Constants.CONF_JDBC_USER)
        val jdbcPassword = ConfigurationManager.config.getString(Constants.CONF_JDBC_PASSWORD)
        val size = ConfigurationManager.config.getInt(Constants.CONF_JDBC_DATASOURCE_SIZE)

        val pooledFactory = new PooledMySqlClientFactory(jdbcUrl, jdbcUser, jdbcPassword)
        val poolConfig = {
          // 创建标准对象池配置类的实例
          val c = new GenericObjectPoolConfig
          // 设置配置对象参数
          // 设置最大对象数
          c.setMaxTotal(size)
          // 设置最大空闲对象数
          c.setMaxIdle(size)
          c
        }
        // 对象池的创建需要工厂类和配置类
        // 返回一个 GenericObjectPool 对象池
        this.genericObjectPool = new GenericObjectPool[MySqlProxy](pooledFactory, poolConfig)
      }
    }
    genericObjectPool
  }
}

