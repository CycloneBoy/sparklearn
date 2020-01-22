package com.cycloneboy.bigdata.stormlearn.common.kafka.wordcount;

/** Create by sl on 2020-01-14 18:07 */
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/** @author ccc */
public class KafkaOutputBolt implements IBasicBolt {

  private Map<String, Integer> wordCounts = new HashMap<>();

  @Override
  public void prepare(Map stormConf, TopologyContext context) {}

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String word = tuple.getStringByField("word");
    Integer counts = tuple.getIntegerByField("counts");
    this.wordCounts.put(word, counts);
  }

  @Override
  public void cleanup() {
    Set<String> keySet = wordCounts.keySet();
    List<String> keyList = new ArrayList<>();
    keyList.addAll(keySet);
    Collections.sort(keyList);
    keyList.forEach(key -> System.out.println(key + "->" + wordCounts.get(key)));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {}

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
