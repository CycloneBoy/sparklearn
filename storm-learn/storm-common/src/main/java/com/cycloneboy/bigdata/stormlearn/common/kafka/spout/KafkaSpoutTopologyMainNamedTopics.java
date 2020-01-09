package com.cycloneboy.bigdata.stormlearn.common.kafka.spout;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

import com.cycloneboy.bigdata.stormlearn.common.kafka.bolt.KafkaProducerTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * This example sets up 3 topologies to put data in Kafka via the KafkaBolt, and shows how to set up
 * a topology that reads from some Kafka topics using the KafkaSpout. Create by sl on 2020-01-09
 * 17:16
 */
public class KafkaSpoutTopologyMainNamedTopics {

  private static final String TOPIC_2_STREAM = "test_2_stream";
  private static final String TOPIC_0_1_STREAM = "test_0_1_stream";
  private static final String KAFKA_LOCAL_BROKER = "localhost:9092";
  public static final String TOPIC_0 = "kafka-spout-test";
  public static final String TOPIC_1 = "kafka-spout-test-1";
  public static final String TOPIC_2 = "kafka-spout-test-2";

  public static void main(String[] args) throws Exception {
    new KafkaSpoutTopologyMainNamedTopics().runMain(args);
  }

  protected void runMain(String[] args) throws Exception {
    final String brokerUrl = args.length > 0 ? args[0] : KAFKA_LOCAL_BROKER;
    System.out.println("Running with broker url: " + brokerUrl);

    Config tpConf = getConfig();

    // Producers. This is just to get some data in Kafka, normally you would be getting this data
    // from elsewhere
    //    StormSubmitter.submitTopology(
    //        TOPIC_0 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_0));
    //    StormSubmitter.submitTopology(
    //        TOPIC_1 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_1));
    //    StormSubmitter.submitTopology(
    //        TOPIC_2 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_2));
    //
    //    // Consumer. Sets up a topology that reads the given Kafka spouts and logs the received
    // messages
    //    StormSubmitter.submitTopology(
    //        "storm-kafka-client-spout-test",
    //        tpConf,
    //        getTopologyKafkaSpout(getKafkaSpoutConfig(brokerUrl)));

    // 5 本地模式提交
    LocalCluster localCluster = new LocalCluster();

    localCluster.submitTopology(
        TOPIC_0 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_0));
    localCluster.submitTopology(
        TOPIC_1 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_1));
    localCluster.submitTopology(
        TOPIC_2 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_2));

    // Consumer. Sets up a topology that reads the given Kafka spouts and logs the received messages
    localCluster.submitTopology(
        "storm-kafka-client-spout-test",
        tpConf,
        getTopologyKafkaSpout(getKafkaSpoutConfig(brokerUrl)));
  }

  protected Config getConfig() {
    Config config = new Config();
    config.setDebug(false);
    return config;
  }

  protected StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
    final TopologyBuilder tp = new TopologyBuilder();
    tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 1);
    tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt())
        .shuffleGrouping("kafka_spout", TOPIC_0_1_STREAM)
        .shuffleGrouping("kafka_spout", TOPIC_2_STREAM);
    tp.setBolt("kafka_bolt_1", new KafkaSpoutTestBolt())
        .shuffleGrouping("kafka_spout", TOPIC_2_STREAM);
    return tp.createTopology();
  }

  protected KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {
    ByTopicRecordTranslator<String, String> trans =
        new ByTopicRecordTranslator<>(
            (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
            new Fields("topic", "partition", "offset", "key", "value"),
            TOPIC_0_1_STREAM);
    trans.forTopic(
        TOPIC_2,
        (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
        new Fields("topic", "partition", "offset", "key", "value"),
        TOPIC_2_STREAM);
    return KafkaSpoutConfig.builder(bootstrapServers, new String[] {TOPIC_0, TOPIC_1, TOPIC_2})
        .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
        .setRetry(getRetryService())
        .setRecordTranslator(trans)
        .setOffsetCommitPeriodMs(10_000)
        .setFirstPollOffsetStrategy(EARLIEST)
        .setMaxUncommittedOffsets(250)
        .build();
  }

  protected KafkaSpoutRetryService getRetryService() {
    return new KafkaSpoutRetryExponentialBackoff(
        TimeInterval.microSeconds(500),
        TimeInterval.milliSeconds(2),
        Integer.MAX_VALUE,
        TimeInterval.seconds(10));
  }
}
