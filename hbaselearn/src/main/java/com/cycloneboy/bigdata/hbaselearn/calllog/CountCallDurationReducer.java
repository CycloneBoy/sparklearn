package com.cycloneboy.bigdata.hbaselearn.calllog;

import com.cycloneboy.bigdata.hbaselearn.calllog.kv.key.ComDimension;
import com.cycloneboy.bigdata.hbaselearn.calllog.kv.value.CountCallDurationValue;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Create by sl on 2019-12-05 13:50 */
public class CountCallDurationReducer
    extends Reducer<ComDimension, Text, ComDimension, CountCallDurationValue> {

  CountCallDurationValue callDurationValue = new CountCallDurationValue();

  @Override
  protected void reduce(ComDimension key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    int callSum = 0;
    int callDurationSum = 0;

    for (Text value : values) {
      callSum++;
      callDurationSum += Integer.parseInt(value.toString());
    }

    callDurationValue.setCallSum(callSum);
    callDurationValue.setCallDurationSum(callDurationSum);
    context.write(key, callDurationValue);
  }
}
