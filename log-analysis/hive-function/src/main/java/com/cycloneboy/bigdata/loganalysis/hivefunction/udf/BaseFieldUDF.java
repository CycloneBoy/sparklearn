package com.cycloneboy.bigdata.loganalysis.hivefunction.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.json.JSONException;
import org.json.JSONObject;;

/** Create by sl on 2020-03-23 09:09 */
@Description(
    name = "BaseFieldUDF",
    value = "_FUNC_(str) - from the input string" + "returns the value that is $str Lower case ",
    extended = "Example:\n" + " > SELECT _FUNC_(str) FROM src;")
public class BaseFieldUDF extends GenericUDF {

  @Override
  public ObjectInspector initialize(ObjectInspector[] objectInspectors)
      throws UDFArgumentException {
    return null;
  }

  @Override
  public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    if (deferredObjects.length != 2) {
      throw new HiveException("invalidate paramters");
    }

    String line = (String) deferredObjects[0].get();
    String jsonkeysString = (String) deferredObjects[1].get();

    // 0 准备一个sb
    StringBuilder sb = new StringBuilder();

    // 1 切割jsonkeys  mid uid vc vn l sr os ar md
    String[] jsonkeys = jsonkeysString.split(",");

    // 2 处理line   服务器时间 | json
    String[] logContents = line.split("\\|");

    // 3 合法性校验
    if (logContents.length != 2 || StringUtils.isBlank(logContents[1])) {
      return "";
    }

    // 4 开始处理json
    try {
      JSONObject jsonObject = new JSONObject(logContents[1]);

      // 获取cm里面的对象
      JSONObject base = jsonObject.getJSONObject("cm");

      // 循环遍历取值
      for (int i = 0; i < jsonkeys.length; i++) {
        String filedName = jsonkeys[i].trim();

        if (base.has(filedName)) {
          sb.append(base.getString(filedName)).append("\t");
        } else {
          sb.append("\t");
        }
      }

      sb.append(jsonObject.getString("et")).append("\t");
      sb.append(logContents[0]).append("\t");
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return sb.toString();
  }

  @Override
  public String getDisplayString(String[] strings) {
    return null;
  }

  public static void main(String[] args) throws HiveException {

    String line =
        "1541217850324|{\"cm\":{\"mid\":\"m7856\",\"uid\":\"u8739\",\"ln\":\"-74.8\",\"sv\":\"V2.2.2\",\"os\":\"8.1.3\",\"g\":\"P7XC9126@gmail.com\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"6\",\"hw\":\"640*960\",\"ar\":\"MX\",\"t\":\"1541204134250\",\"la\":\"-31.7\",\"md\":\"huawei-17\",\"vn\":\"1.1.2\",\"sr\":\"O\",\"ba\":\"Huawei\"},\"ap\":\"weather\",\"et\":[{\"ett\":\"1541146624055\",\"en\":\"display\",\"kv\":{\"goodsid\":\"n4195\",\"copyright\":\"ESPN\",\"content_provider\":\"CNN\",\"extend2\":\"5\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"3\",\"showtype\":\"2\",\"category\":\"72\",\"newstype\":\"5\"}},{\"ett\":\"1541213331817\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"15\",\"action\":\"3\",\"extend1\":\"\",\"type1\":\"\",\"type\":\"3\",\"loading_way\":\"1\"}},{\"ett\":\"1541126195645\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"0\",\"action\":\"2\",\"detail\":\"325\",\"source\":\"4\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"5\"}},{\"ett\":\"1541202678812\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1541184614380\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1541194686688\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}}]}";
    DeferredObject[] input = new DeferredJavaObject[2];
    input[0] = new DeferredJavaObject(line);
    input[1] = new DeferredJavaObject("mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
    Object evaluate = new BaseFieldUDF().evaluate(input);
    System.out.println(evaluate.toString());
  }
}
