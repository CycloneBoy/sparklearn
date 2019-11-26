package com.cycloneboy.bigdata.businessanalysis.commons.utils

/**
 *
 * Create by  sl on 2019-11-25 14:50
 */
object ValidUtils {
  /**
   * 校验数据中的指定字段，是否在指定范围内
   *
   * @param data            数据
   * @param dataField       数据字段
   * @param parameter       参数
   * @param startParamField 起始参数字段
   * @param endParamField   结束参数字段
   * @return 校验结果
   */
  def between(data: String, dataField: String, parameter: String, startParamField: String, endParamField: String): Boolean = {

    val startParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", startParamField)
    val endParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", endParamField)
    if (startParamFieldStr == null || endParamFieldStr == null) {
      return true
    }

    val startParamFieldValue = startParamFieldStr.toInt
    val endParamFieldValue = endParamFieldStr.toInt

    val dataFieldStr = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if (dataFieldStr != null) {
      val dataFieldValue = dataFieldStr.toInt
      if (dataFieldValue >= startParamFieldValue && dataFieldValue <= endParamFieldValue) {
        return true
      } else {
        return false
      }
    }
    false
  }

  /**
   * 校验数据中的指定字段，是否有值与参数字段的值相同
   *
   * @param data       数据
   * @param dataField  数据字段
   * @param parameter  参数
   * @param paramField 参数字段
   * @return 校验结果
   */
  def in(data: String, dataField: String, parameter: String, paramField: String): Boolean = {
    val paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if (paramFieldValue == null) {
      return true
    }
    val paramFieldValueSplited = paramFieldValue.split(",")

    val dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if (dataFieldValue != null && dataFieldValue != "-1") {
      val dataFieldValueSplited = dataFieldValue.split(",")

      for (singleDataFieldValue <- dataFieldValueSplited) {
        for (singleParamFieldValue <- paramFieldValueSplited) {
          if (singleDataFieldValue.compareTo(singleParamFieldValue) == 0) {
            return true
          }
        }
      }
    }
    false
  }

  /**
   * 校验数据中的指定字段，是否在指定范围内
   *
   * @param data       数据
   * @param dataField  数据字段
   * @param parameter  参数
   * @param paramField 参数字段
   * @return 校验结果
   */
  def equal(data: String, dataField: String, parameter: String, paramField: String): Boolean = {
    val paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if (paramFieldValue == null) {
      return true
    }

    val dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if (dataFieldValue != null) {
      if (dataFieldValue.compareTo(paramFieldValue) == 0) {
        return true
      }
    }
    false
  }

  /**
   * 校验数据中的指定字段，是否有值与参数字段的值相同
   *
   * @param source 数据
   * @param target 数据字段
   * @return 校验结果
   */
  def in(source: String, target: String, comma: String = ","): Boolean = {

    if (StringUtils.isNotEmpty(source) && StringUtils.isNotEmpty(target)) {
      val sourceSet = source.split(comma).toSet
      val targetSet = target.split(comma).toSet

      val resultSet: Set[String] = sourceSet & targetSet
      resultSet.nonEmpty
    } else {
      true
    }
  }
}
