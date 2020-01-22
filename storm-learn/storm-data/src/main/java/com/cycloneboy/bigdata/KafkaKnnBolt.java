package com.cycloneboy.bigdata;

/** Create by sl on 2020-01-14 18:06 */
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/** @author ccc */
public class KafkaKnnBolt implements IBasicBolt {

  //  private KnnAlgo knn;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    //    try {
    //      //      knn = new KnnAlgo("viewlogTrain.txt");
    //    } catch (IOException e) {
    //      e.printStackTrace();
    //    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String viewLog = tuple.getStringByField("value");
    //    System.out.println("测试:" + viewLog);

    int type = -1;
    //    type = knn.testKnn(viewLog, 5);

    System.out.println("测试:" + viewLog + " - 分类类别: " + type);
    //    collector.emit(new Values(viewLog, type));
  }

  @Override
  public void cleanup() {}

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //    declarer.declare(new Fields("word", "counts"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
