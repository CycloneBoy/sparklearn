package com.cycloneboy.bigdata.communication.mapper;

import com.cycloneboy.bigdata.communication.kv.key.ComDimension;
import com.cycloneboy.bigdata.communication.kv.key.ContactDimension;
import com.cycloneboy.bigdata.communication.kv.key.DateDimension;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

/** Create by sl on 2019-12-05 13:50 */
public class CountCallDurationMapper extends TableMapper<ComDimension, Text> {

  private ComDimension comDimension = new ComDimension();
  private Text durationText = new Text();
  private Map<String, String> phoneNameMap;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    phoneNameMap = new HashMap<>();
    phoneNameMap.put("17078388295", "李雁");
    phoneNameMap.put("13980337439", "卫艺");
    phoneNameMap.put("14575535933", "仰莉");
    phoneNameMap.put("19902496992", "陶欣悦");
    phoneNameMap.put("18549641558", "施梅梅");
    phoneNameMap.put("17005930322", "金虹霖");
    phoneNameMap.put("18468618874", "魏明艳");
    phoneNameMap.put("18576581848", "华贞");
    phoneNameMap.put("15978226424", "华啟倩");
    phoneNameMap.put("15542823911", "仲采绿");
    phoneNameMap.put("17526304161", "卫丹");
    phoneNameMap.put("15422018558", "戚丽红");
    phoneNameMap.put("17269452013", "何翠柔");
    phoneNameMap.put("17764278604", "钱溶艳");
    phoneNameMap.put("15711910344", "钱琳");
    phoneNameMap.put("15714728273", "缪静欣");
    phoneNameMap.put("16061028454", "焦秋菊");
    phoneNameMap.put("16264433631", "吕访琴");
    phoneNameMap.put("17601615878", "沈丹");
    phoneNameMap.put("15897468949", "褚美丽");
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
    // 05_19902496992_20170312154840_15542823911_1_1288
    String rowKey = Bytes.toString(key.get());
    String[] splits = rowKey.split("_");

    // 暂时不处理被叫号码话单
    if (splits[4].equals("0")) return;
    // 以下数据全部是主叫数据，但是也包含了被叫电话的数据
    String caller = splits[1];
    String callee = splits[3];
    String buildTime = splits[2];
    String duration = splits[5];

    durationText.set(duration);

    // 20170312154840
    String year = buildTime.substring(0, 4);
    String month = buildTime.substring(4, 6);
    String day = buildTime.substring(6, 8);

    // 组装ComDimension
    // 组装DateDimension
    //// 05_19902496992_20170312154840_15542823911_1_1288
    // 一条话单是一天要统计的数据也是一个月要统计的数据也是一年要统计的数据
    DateDimension yearDimension = new DateDimension(Integer.parseInt(year), -1, -1);
    DateDimension monthDimension =
        new DateDimension(Integer.parseInt(year), Integer.parseInt(month), -1);
    DateDimension dayDimension =
        new DateDimension(Integer.parseInt(year), Integer.parseInt(month), Integer.parseInt(day));

    // 组装ContactDimension
    // 发送主叫数据
    sendData(context, caller, yearDimension, monthDimension, dayDimension);
    // 发送被叫数据
    sendData(context, callee, yearDimension, monthDimension, dayDimension);
  }

  /**
   * 发送数据
   *
   * @param context
   * @param callee
   * @param yearDimension
   * @param monthDimension
   * @param dayDimension
   * @throws IOException
   * @throws InterruptedException
   */
  private void sendData(
      Context context,
      String callee,
      DateDimension yearDimension,
      DateDimension monthDimension,
      DateDimension dayDimension)
      throws IOException, InterruptedException {
    // 开始聚合被叫数据
    ContactDimension calleeContactDimension =
        new ContactDimension(callee, phoneNameMap.get(callee));

    // 开始聚合主叫数据
    comDimension.setContactDimension(calleeContactDimension);
    // 年
    comDimension.setDateDimension(yearDimension);
    context.write(comDimension, durationText);
    // 月
    comDimension.setDateDimension(monthDimension);
    context.write(comDimension, durationText);
    // 日
    comDimension.setDateDimension(dayDimension);
    context.write(comDimension, durationText);
  }
}
