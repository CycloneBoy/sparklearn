package com.cycloneboy.bigdata.stormlearn.common.kafka.wordcount;

/** Create by sl on 2020-01-14 18:06 */
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/** @author ccc */
public class KafkaSplitSentenceBolt implements IBasicBolt {

  @Override
  public void prepare(Map stormConf, TopologyContext context) {}

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String poetry = tuple.getStringByField("value");
    System.out.println("测试:" + poetry);

    //    List<String> sentences = Arrays.asList(poetry.split(","));
    //    sentences.forEach(
    //        sentence -> {
    //          List<String> words = Arrays.asList(sentence.replace("。", "").split(""));
    //          words.forEach(word -> collector.emit(new Values(word)));
    //        });
  }

  @Override
  public void cleanup() {}

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
