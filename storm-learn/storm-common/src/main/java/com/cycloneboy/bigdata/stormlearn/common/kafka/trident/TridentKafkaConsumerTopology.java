package com.cycloneboy.bigdata.stormlearn.common.kafka.trident;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create by sl on 2020-01-09 17:19 */
public class TridentKafkaConsumerTopology {

  protected static final Logger LOG = LoggerFactory.getLogger(TridentKafkaConsumerTopology.class);

  /**
   * Creates a new topology that prints inputs to stdout.
   *
   * @param tridentSpout The spout to use
   */
  public static StormTopology newTopology(ITridentDataSource tridentSpout) {
    final TridentTopology tridentTopology = new TridentTopology();
    final Stream spoutStream = tridentTopology.newStream("spout", tridentSpout).parallelismHint(2);
    spoutStream.each(spoutStream.getOutputFields(), new Debug(false));
    return tridentTopology.build();
  }
}
