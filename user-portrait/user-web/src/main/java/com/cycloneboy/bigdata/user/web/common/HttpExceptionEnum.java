package com.cycloneboy.bigdata.user.web.common;

import lombok.Getter;

/**
 * create by CycloneBoy on 2019-05-26 17:27
 */
@Getter
public enum HttpExceptionEnum {
  SUCCESS("0", "Operation success.", "success"),

  FAILED("1", "Operation failed.", "failed"),

  REMOTE_API_FAILED("10001", "Remote api :Operation failed.", "failed");

  private String code;

  private String message;

  private String desc;

  HttpExceptionEnum(String code, String message) {
    this.code = code;
    this.message = message;
  }

  HttpExceptionEnum(String code, String message, String desc) {
    this.code = code;
    this.message = message;
    this.desc = desc;
  }
}
