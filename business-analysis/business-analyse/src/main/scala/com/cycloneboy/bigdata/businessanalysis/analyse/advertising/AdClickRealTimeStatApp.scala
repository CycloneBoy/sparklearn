package com.cycloneboy.bigdata.businessanalysis.analyse.advertising

import java.lang
import java.util.{Date, UUID}

import com.cycloneboy.bigdata.businessanalysis.analyse.dao.{AdBlackListDao, AdUserClickCountDao}
import com.cycloneboy.bigdata.businessanalysis.analyse.model.{AdBlacklist, AdUserClickCount}
import com.cycloneboy.bigdata.businessanalysis.commons.common.Constants
import com.cycloneboy.bigdata.businessanalysis.commons.conf.ConfigurationManager
import com.cycloneboy.bigdata.businessanalysis.commons.utils.DateUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
 *
 * Create by  sl on 2019-11-28 14:15
 */
object AdClickRealTimeStatApp {

  def main(args: Array[String]): Unit = {


    val taskUUID = DateUtils.getTodayStandard() + "_" + UUID.randomUUID().toString.replace("-", "")

    val sparkConf = new SparkConf().setAppName("AdClickRealTimeStatApp").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("error")

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("hdfs://localhost:9000/user/spark/checkpoint")

    val brokerList = ConfigurationManager.config.getString(Constants.CONF_KAFKA_BROKER_LIST)
    val topics = ConfigurationManager.config.getString(Constants.CONF_KAFKA_TOPICS)

    val kafkaParam = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "sl-business-consumer-group",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
    )

    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题

    val adRealTimeLogDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topics), kafkaParam))

    var adRealTimeValueDStream = adRealTimeLogDStream.map(consumerRecordRDD => consumerRecordRDD.value())
    // 用于Kafka Stream的线程非安全问题，重新分区切断血统
    adRealTimeValueDStream = adRealTimeValueDStream.repartition(400)

    // 打印Kafka数据,确保数据通路
    //    adRealTimeValueDStream.print(5)

    // 根据动态黑名单进行数据过滤 (userid, timestamp province city userid adid)
    val filteredAdRealTimeLogDStream: DStream[(Long, String)] = fliterByBlacklist(spark, adRealTimeValueDStream)

    println("---------filteredAdRealTimeLogDStream--------")
    filteredAdRealTimeLogDStream.print(3)
    println("-----------------")

    // 业务功能一：生成动态黑名单
    generateDynamicBlacklist(filteredAdRealTimeLogDStream)

    ssc.start()
    ssc.awaitTermination()
  }


  /**
   * 生成动态黑名单
   *
   * @param filteredAdRealTimeLogDStream
   */
  def generateDynamicBlacklist(filteredAdRealTimeLogDStream: DStream[(Long, String)]) = {
    // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量
    // 通过对原始实时日志的处理
    // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
    val dailyUserAdClickDStream: DStream[(String, Long)] = filteredAdRealTimeLogDStream.map { case (userid, log) =>
      val logSplited = log.split(" ")
      val timestamp = logSplited(0)
      val date = new Date(timestamp.toLong)
      val datekey = DateUtils.formatDateKey(date)

      val userid = logSplited(3).toLong
      val adid = logSplited(4)

      val key = datekey + "_" + userid + "_" + adid
      (key, 1L)
    }

    println("---------dailyUserAdClickDStream--------")
    dailyUserAdClickDStream.print(3)
    println("-----------------")


    val dailyUserAdClickCountDStream: DStream[(String, Long)] = dailyUserAdClickDStream.reduceByKey(_ + _)

    // 源源不断的，每个5s的batch中，当天每个用户对每支广告的点击次数
    // <yyyyMMdd_userid_adid, clickCount>
    dailyUserAdClickCountDStream.foreachRDD { rdd =>

      rdd.foreachPartition { items =>
        // 对每个分区的数据就去获取一次连接对象
        // 每次都是从连接池中获取，而不是每次都创建
        // 写数据库操作，性能已经提到最高了
        val adUserClickCounts = ArrayBuffer[AdUserClickCount]()
        for (item <- items) {
          val keySplited = item._1.split("_")
          // yyyy-MM-dd
          val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
          val userid = keySplited(1).toLong
          val adid = keySplited(2).toLong
          val clickCount = item._2

          // 批量插入
          adUserClickCounts += AdUserClickCount(date, userid, adid, clickCount)
        }
        AdUserClickCountDao.updateBatch(adUserClickCounts.toArray)
      }
    }

    println("---------dailyUserAdClickCountDStream--------")
    dailyUserAdClickCountDStream.print(3)
    println("-----------------")

    val adBlacklistCountMax = ConfigurationManager.config.getInt(Constants.CONF_AD_BLACK_LIST_FILTER_MAX)

    // 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
    // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
    // 从mysql中查询
    // 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
    // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化
    val blacklistDStream: DStream[(String, Long)] = dailyUserAdClickCountDStream.filter { case (key, count) =>
      val keySplited = key.split("_")
      // yyyy-MM-dd
      val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
      val userid = keySplited(1).toLong
      val adid = keySplited(2).toLong

      // 从mysql中查询指定日期指定用户对指定广告的点击量
      val clickCount = AdUserClickCountDao.findClickCountByMutiKey(date, userid, adid)
      if (clickCount >= adBlacklistCountMax) {
        true
      } else {
        false
      }
    }

    println("---------blacklistDStream--------")
    blacklistDStream.print(3)
    println("-----------------")

    // blacklistDStream
    // 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
    // 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
    // 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
    // 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
    // 所以直接插入mysql即可

    // 我们在插入前要进行去重
    // yyyyMMdd_userid_adid
    // 20151220_10001_10002 100
    // 20151220_10001_10003 100
    // 10001这个userid就重复了

    // 实际上，是要通过对dstream执行操作，对其中的rdd中的userid进行全局的去重, 返回Userid
    val blacklistUseridDStream: DStream[Long] = blacklistDStream.map(item => item._1.split("_")(1).toLong)
    //    ProcessUtils.printRDD(blacklistUseridDStream)
    println("---------blacklistUseridDStream--------")
    blacklistUseridDStream.print(3)
    println("-----------------")


    val distinctBlacklistUseridDStream: DStream[Long] = blacklistUseridDStream.transform(useridStream => useridStream.distinct())
    println("---------distinctBlacklistUseridDStream--------")
    distinctBlacklistUseridDStream.print(3)
    println("-----------------")

    // 到这一步为止，distinctBlacklistUseridDStream
    // 每一个rdd，只包含了userid，而且还进行了全局的去重，保证每一次过滤出来的黑名单用户都没有重复的

    distinctBlacklistUseridDStream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        val adBlacklists = ArrayBuffer[AdBlacklist]()

        for (item <- items) {
          adBlacklists += AdBlacklist(item)
        }
        //        println("----------------------保存AdBlackListDao------------------------------" + adBlacklists.toArray.toString)
        AdBlackListDao.insertBatch(adBlacklists.toArray)
      }
    }


  }

  /**
   * 根据黑名单进行过滤
   *
   * @param spark
   * @param adRealTimeValueDStream
   */
  def fliterByBlacklist(spark: SparkSession, adRealTimeValueDStream: DStream[String]) = {
    // 刚刚接受到原始的用户点击行为日志之后
    // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
    // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）
    val filteredAdRealTimeLogDStream: DStream[(Long, String)] = adRealTimeValueDStream.transform { consumerRecordRDD =>

      val adBlacklists: Array[AdBlacklist] = AdBlackListDao.findAll()

      val blacklistRDD: RDD[(Long, Boolean)] = spark.sparkContext.makeRDD(adBlacklists.map(item => (item.userid, true)))

      val mappedRDD: RDD[(Long, String)] = consumerRecordRDD.map(consumerRecord => {
        val userid: Long = consumerRecord.split(" ")(3).toLong
        (userid, consumerRecord)
      })


      // 将原始日志数据rdd，与黑名单rdd，进行左外连接
      // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
      // 用inner join，内连接，会导致数据丢失
      val joinedRDD: RDD[(Long, (String, Option[Boolean]))] = mappedRDD.leftOuterJoin(blacklistRDD)

      val filteredRDD: RDD[(Long, (String, Option[Boolean]))] = joinedRDD.filter { case (userid, (log, black)) =>
        // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
        if (black.isDefined && black.get) false else true
      }

      filteredRDD.map { case (userid, (log, black)) => (userid, log) }
    }

    filteredAdRealTimeLogDStream
  }


}
