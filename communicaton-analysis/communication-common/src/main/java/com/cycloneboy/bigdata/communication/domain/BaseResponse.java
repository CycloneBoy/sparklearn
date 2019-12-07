package com.cycloneboy.bigdata.communication.domain;

import com.cycloneboy.bigdata.communication.common.HttpExceptionEnum;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * create by CycloneBoy on 2019-05-26 14:11
 *
 * @author CycloneBoy
 */
@Data
@AllArgsConstructor
public class BaseResponse {

  private String code;

  private String message;

  private Object data;

  public static BaseResponse ok() {
    return new BaseResponse();
  }

  public static BaseResponse failed() {
    return new BaseResponse(HttpExceptionEnum.FAILED);
  }

  public static BaseResponse failed(String message) {
    return new BaseResponse(HttpExceptionEnum.FAILED, message);
  }

  public BaseResponse() {
    this.code = HttpExceptionEnum.SUCCESS.getCode();
    this.message = HttpExceptionEnum.SUCCESS.getMessage();
    this.data = "";
  }

  public BaseResponse(Object data) {
    this.code = HttpExceptionEnum.SUCCESS.getCode();
    this.message = HttpExceptionEnum.SUCCESS.getMessage();
    this.data = data;
  }

  public BaseResponse(HttpExceptionEnum exception) {
    this.code = exception.getCode();
    this.message = exception.getMessage();
    this.data = "";
  }

  public BaseResponse(HttpExceptionEnum exception, String message) {
    this.code = exception.getCode();
    this.message = message;
    this.data = "";
  }

  public BaseResponse(HttpExceptionEnum exception, Object data) {
    this.code = exception.getCode();
    this.message = exception.getMessage();
    this.data = data;
  }
}
