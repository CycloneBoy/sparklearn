package com.cycloneboy.bigdata.user.web.config.aop;

import com.cycloneboy.bigdata.user.web.common.CloudException;
import com.cycloneboy.bigdata.user.web.config.annotation.ResponseAnnotation;
import com.cycloneboy.bigdata.user.web.domain.BaseResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Create by  sl on 2021-03-28 10:45
 */
@ControllerAdvice(annotations = ResponseAnnotation.class)
@ResponseBody
@Slf4j
public class ExceptionHandlerAdvice {

  /**
   * 处理未捕获的Exception
   *
   * @param e 异常
   * @return 统一响应体
   */
  @ExceptionHandler(Exception.class)
  public BaseResponse handleException(Exception e) {
    log.error(e.getMessage(), e);
    return BaseResponse.failed(e.getMessage());
  }

  /**
   * 处理未捕获的RuntimeException
   *
   * @param e 运行时异常
   * @return 统一响应体
   */
  @ExceptionHandler(RuntimeException.class)
  public BaseResponse handleRuntimeException(RuntimeException e) {
    log.error(e.getMessage(), e);
    return BaseResponse.failed(e.getMessage());
  }

  /**
   * 处理业务异常BaseException
   *
   * @param e 业务异常
   * @return 统一响应体
   */
  @ExceptionHandler(CloudException.class)
  public BaseResponse handleBaseException(CloudException e) {
    log.error(e.getMessage(), e);
    return new BaseResponse(e.getCode(), e.getMessage(), e.getDesc());
  }
}
