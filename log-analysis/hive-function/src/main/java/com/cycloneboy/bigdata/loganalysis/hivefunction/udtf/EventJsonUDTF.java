package com.cycloneboy.bigdata.loganalysis.hivefunction.udtf;

import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * lateral view flat_analizer(ops) tmp_k as event_name, event_json;
 *
 * <p>Create by sl on 2020-01-15 22:29
 */
public class EventJsonUDTF extends GenericUDTF {

  /**
   * 该方法中，我们将指定输出参数的名称和参数类型：
   *
   * @param argOIs
   * @return
   * @throws UDFArgumentException
   */
  @Override
  public StructObjectInspector initialize(StructObjectInspector argOIs)
      throws UDFArgumentException {
    ArrayList<String> fieldNames = new ArrayList<>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();

    fieldNames.add("event_name");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    fieldNames.add("event_json");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  /**
   * 输入1条记录，输出若干条结果
   *
   * @param objects
   * @throws HiveException
   */
  @Override
  public void process(Object[] objects) throws HiveException {
    // 获取传入的et
    String input = objects[0].toString();

    // 如果传进来的数据为空，直接返回过滤掉该数据
    if (StringUtils.isBlank(input)) {
      return;
    } else {

      try {
        // 获取一共有几个事件（ad/facoriters）
        JSONArray ja = new JSONArray(input);

        if (ja == null) return;

        // 循环遍历每一个事件
        for (int i = 0; i < ja.length(); i++) {
          String[] result = new String[2];

          try {
            // 取出每个的事件名称（ad/facoriters）
            result[0] = ja.getJSONObject(i).getString("en");

            // 取出每一个事件整体
            result[1] = ja.getString(i);
          } catch (JSONException e) {
            continue;
          }

          // 将结果返回
          forward(result);
        }
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void close() throws HiveException {}
}
