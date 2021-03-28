package com.cycloneboy.bigdata.user.web.config.aop;

import com.cycloneboy.bigdata.user.web.domain.BaseResponse;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Create by  sl on 2021-03-28 09:45
 */
@Slf4j
@Aspect
@Component
public class ResultAop {

  private final String pointcutStr = "execution( * com.cycloneboy.bigdata.user.web.controller.*.*(..))";

  @Value(value = "${devMode:true}")
  private boolean devMode;

  @Pointcut(pointcutStr)
  public void excuteController() {

  }


  @AfterReturning(value = pointcutStr, returning = "jobj")
  public BaseResponse doAroundAdvice(JoinPoint joinPoint, Object jobj) {
    // proceedingJoinPoint.getArgs();
    try {
      // obj之前可以写目标方法执行前的逻辑

      log.info(String.format("begin:%s", jobj.toString()));
      BaseResponse baseResponse = procesResultObj(jobj);
      log.info(String.format("end:%s", baseResponse.toString()));
      return baseResponse;
    } catch (Throwable throwable) {
      throwable.printStackTrace();
      log.error("@AfterReturning 出错{}", throwable.getMessage());
    }
    return BaseResponse.failed();
  }

  /**
   * 处理返回对象
   */
  @SuppressWarnings("unused")
  private BaseResponse procesResultObj(Object obj) {
    return BaseResponse.ok(obj);
  }


}
