package com.cycloneboy.bigdata.stormlearn.common.kafka.wordcount;

/** Create by sl on 2020-01-14 18:05 */
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/** @author ccc */
public class KafkaWordCountTopology {

  private static final String TOPICS = "word-count";

  private static final String KEYS = "word";

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";

  private static final String KAFKA_WORD_COUT_SPOUT_ID = "KafkaWordCountSpout";

  private static final String SPLIT_SENTENCE_BOLT_ID = "SplitSentenceBolt";

  private static final String KAFKA_WORD_COUNT_BOLT_ID = "KafkaWordCountBolt";

  private static final String KAFKA_OUTPUT_BOLT_ID = "KafkaOutputBolt";

  public static void main(String[] args) throws Exception {
    KafkaSpoutConfig<String, String> kafkaSpoutConfig =
        KafkaSpoutConfig.builder(BOOTSTRAP_SERVERS, TOPICS)
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, "KafkaWordCountSpoutGroup")
            .setTupleTrackingEnforced(true)
            .build();
    KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);

    TopologyBuilder tp = new TopologyBuilder();
    tp.setSpout(KAFKA_WORD_COUT_SPOUT_ID, kafkaSpout, 2);
    tp.setBolt(SPLIT_SENTENCE_BOLT_ID, new KafkaSplitSentenceBolt(), 2)
        .setNumTasks(2)
        .shuffleGrouping(KAFKA_WORD_COUT_SPOUT_ID);
    tp.setBolt(KAFKA_WORD_COUNT_BOLT_ID, new KafkaWordCountBolt(), 4)
        .setNumTasks(2)
        .fieldsGrouping(SPLIT_SENTENCE_BOLT_ID, new Fields(KEYS));
    tp.setBolt(KAFKA_OUTPUT_BOLT_ID, new KafkaOutputBolt(), 2)
        .setNumTasks(2)
        .globalGrouping(KAFKA_WORD_COUNT_BOLT_ID);

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      // 提交集群
      conf.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, tp.createTopology());
    } else {
      // 本地测试
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("kafka-word-count-topology", conf, tp.createTopology());
      Thread.sleep(10000);
      cluster.shutdown();
    }
  }
}
