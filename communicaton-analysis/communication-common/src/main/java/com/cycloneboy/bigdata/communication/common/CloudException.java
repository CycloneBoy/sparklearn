package com.cycloneboy.bigdata.communication.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** 自定义异常类 create by CycloneBoy on 2019-06-20 22:38 */
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class CloudException extends RuntimeException {

  private String message;
  private String desc;
  private String code;

  public CloudException(String message) {
    super(message);
    this.message = message;
  }

  public CloudException(HttpExceptionEnum exception) {
    super(exception.getMessage());
    this.code = exception.getCode();
  }

  public CloudException(HttpExceptionEnum exception, String message) {
    super(message);
    this.code = exception.getCode();
  }

  public CloudException(String message, Throwable e) {
    super(message, e);
    this.message = message;
  }

  public CloudException(String code, String message) {
    super(message);
    this.message = message;
    this.code = code;
  }

  public CloudException(String message, String code, Throwable e) {
    super(message, e);
    this.message = message;
    this.code = code;
  }
}
