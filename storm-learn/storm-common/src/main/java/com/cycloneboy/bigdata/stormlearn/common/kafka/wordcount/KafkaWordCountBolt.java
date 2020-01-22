package com.cycloneboy.bigdata.stormlearn.common.kafka.wordcount;

/** Create by sl on 2020-01-14 18:06 */
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/** @author ccc */
public class KafkaWordCountBolt implements IBasicBolt {

  private Map<String, Integer> wordCounts = new HashMap<>();

  @Override
  public void prepare(Map stormConf, TopologyContext context) {}

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String word = tuple.getStringByField("word");
    Integer counts = wordCounts.get(word);
    if (counts == null) {
      counts = 0;
    }
    counts++;
    wordCounts.put(word, counts);
    collector.emit(new Values(word, counts));
  }

  @Override
  public void cleanup() {}

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "counts"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
