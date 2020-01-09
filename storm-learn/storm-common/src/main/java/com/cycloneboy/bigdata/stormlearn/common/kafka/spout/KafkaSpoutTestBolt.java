package com.cycloneboy.bigdata.stormlearn.common.kafka.spout;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create by sl on 2020-01-09 17:16 */
public class KafkaSpoutTestBolt extends BaseRichBolt {

  protected static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTestBolt.class);

  private OutputCollector collector;

  @Override
  public void prepare(
      Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    LOG.debug("input = [" + input + "]");
    collector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
