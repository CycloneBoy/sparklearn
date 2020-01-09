package com.cycloneboy.bigdata.stormlearn.common.kafka.trident;

/** Create by sl on 2020-01-09 17:18 */
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

import com.cycloneboy.bigdata.stormlearn.common.kafka.bolt.KafkaProducerTopology;
import java.io.Serializable;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutTransactional;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * This example sets up a few topologies to put random strings in Kafka topics via the KafkaBolt,
 * and shows how to set up a Trident topology that reads from some Kafka topics using the
 * KafkaSpout.
 */
public class TridentKafkaClientTopologyNamedTopics {

  private static final String TOPIC_1 = "test-trident";
  private static final String TOPIC_2 = "test-trident-1";
  private static final String KAFKA_LOCAL_BROKER = "localhost:9092";

  private KafkaTridentSpoutOpaque<String, String> newKafkaTridentSpoutOpaque(
      KafkaTridentSpoutConfig<String, String> spoutConfig) {
    return new KafkaTridentSpoutOpaque<>(spoutConfig);
  }

  private KafkaTridentSpoutTransactional<String, String> newKafkaTridentSpoutTransactional(
      KafkaTridentSpoutConfig<String, String> spoutConfig) {
    return new KafkaTridentSpoutTransactional<>(spoutConfig);
  }

  private static final Func<ConsumerRecord<String, String>, List<Object>> JUST_VALUE_FUNC =
      new JustValueFunc();

  /** Needs to be serializable. */
  private static class JustValueFunc
      implements Func<ConsumerRecord<String, String>, List<Object>>, Serializable {

    @Override
    public List<Object> apply(ConsumerRecord<String, String> record) {
      return new Values(record.value());
    }
  }

  protected KafkaTridentSpoutConfig<String, String> newKafkaSpoutConfig(String bootstrapServers) {
    return KafkaTridentSpoutConfig.builder(bootstrapServers, TOPIC_1, TOPIC_2)
        .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 200)
        .setRecordTranslator(JUST_VALUE_FUNC, new Fields("str"))
        .setFirstPollOffsetStrategy(EARLIEST)
        .build();
  }

  public static void main(String[] args) throws Exception {
    new TridentKafkaClientTopologyNamedTopics().run(args);
  }

  protected void run(String[] args)
      throws AlreadyAliveException, InvalidTopologyException, AuthorizationException,
          InterruptedException {
    final String brokerUrl = args.length > 0 ? args[0] : KAFKA_LOCAL_BROKER;
    final boolean isOpaque = args.length > 1 ? Boolean.parseBoolean(args[1]) : true;
    System.out.println("Running with broker url " + brokerUrl + " and isOpaque=" + isOpaque);

    Config tpConf = new Config();
    tpConf.setDebug(true);
    tpConf.setMaxSpoutPending(5);

    // Producers
    StormSubmitter.submitTopology(
        TOPIC_1 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_1));
    StormSubmitter.submitTopology(
        TOPIC_2 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, TOPIC_2));
    // Consumer
    KafkaTridentSpoutConfig<String, String> spoutConfig = newKafkaSpoutConfig(brokerUrl);
    ITridentDataSource spout =
        isOpaque
            ? newKafkaTridentSpoutOpaque(spoutConfig)
            : newKafkaTridentSpoutTransactional(spoutConfig);
    StormSubmitter.submitTopology(
        "topics-consumer", tpConf, TridentKafkaConsumerTopology.newTopology(spout));
  }
}
