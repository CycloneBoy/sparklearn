package com.cycloneboy.bigdata.user.web.filter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Create by sl on 2019-12-08 14:20
 */
@Component
public class AccessControlAllowOriginFilter implements HandlerInterceptor {

  @Override
  public void afterCompletion(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
      throws Exception {

    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Allow-Methods", "POST, GET,PUT, OPTIONS, DELETE");
    response.setHeader("Access-Control-Allow-Credentials", "true");
  }
}
