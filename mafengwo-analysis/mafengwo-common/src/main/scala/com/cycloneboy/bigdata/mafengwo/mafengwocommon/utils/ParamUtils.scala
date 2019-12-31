package com.cycloneboy.bigdata.mafengwo.mafengwocommon.utils

import net.sf.json.JSONObject


/**
 *
 * Create by  sl on 2019-11-25 14:48
 */
object ParamUtils {

  /**
   * 从JSON对象中提取参数
   *
   * @param jsonObject JSON对象
   * @return 参数
   */
  def getParam(jsonObject: JSONObject, field: String): String = {
    jsonObject.getString(field)
    /*val jsonArray = jsonObject.getJSONArray(field)
    if(jsonArray != null && jsonArray.size() > 0) {
      return jsonArray.getString(0)
    }
    null*/
  }
}
