package com.cycloneboy.bigdata.businessanalysis.data.kafka

import java.util.Properties

import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.conf.ConfigurationManager
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 *
 * Create by  sl on 2019-11-25 22:22
 *
 * 利用kafka生成模拟实时数据,广告点击数据
 */
object MockDataOfBusinessOnline {

  /**
   * 模拟的数据
   * 时间点: 当前时间毫秒
   * userId: 0 - 99
   * 省份、城市 ID相同 ： 1 - 9
   * adid: 0 - 19
   * ((0L,"北京","北京"),(1L,"上海","上海"),(2L,"南京","江苏省"),(3L,"广州","广东省"),(4L,"三亚","海南省"),(5L,"武汉","湖北省"),(6L,"长沙","湖南省"),(7L,"西安","陕西省"),(8L,"成都","四川省"),(9L,"哈尔滨","东北省"))
   * 格式 ：timestamp province city userid adid
   * 某个时间点 某个省份 某个城市 某个用户 某个广告
   */
  def generateMockData(): Array[String] = {
    val array = ArrayBuffer[String]()
    val random = new Random()
    // 模拟实时数据：
    // timestamp province city userid adid
    for (i <- 0 to 50) {

      val timestamp = System.currentTimeMillis()
      val province = random.nextInt(Constants.MOCK_NUMBER_OF_CITY)
      val city = province
      val adid = random.nextInt(Constants.MOCK_NUMBER_OF_AD)
      val userid = random.nextInt(Constants.MOCK_NUMBER_OF_USER)

      // 拼接实时数据
      array += timestamp + " " + province + " " + city + " " + userid + " " + adid
    }
    array.toArray
  }

  /**
   * 创建kafka生产着
   *
   * @param broker broker 列表
   * @return KafkaProducer
   */
  def createKafkaProducer(broker: String): KafkaProducer[String, String] = {

    // 创建配置对象
    val props = new Properties()
    // 添加配置
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "MockRealTimeData")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // 根据配置创建Kafka生产者
    new KafkaProducer[String, String](props)
  }


  def main(args: Array[String]): Unit = {

    // 获取配置文件commerce.properties中的Kafka配置参数
    val broker = ConfigurationManager.config.getString(Constants.CONF_KAFKA_BROKER_LIST)
    val topic = ConfigurationManager.config.getString(Constants.CONF_KAFKA_TOPICS)

    val random = new Random()
    // 创建Kafka消费者
    val kafkaProducer = createKafkaProducer(broker)

    while (true) {
      // 随机产生实时数据并通过Kafka生产者发送到Kafka集群中
      for (item <- generateMockData()) {
        kafkaProducer.send(new ProducerRecord[String, String](topic, item))
      }
      Thread.sleep(random.nextInt(3000) + 3000)
    }
  }
}
