package com.cycloneboy.bigdata.user.web.config;

import com.cycloneboy.bigdata.user.web.filter.AccessControlAllowOriginFilter;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Create by sl on 2019-12-08 14:26
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

  //  @Autowired private AccessControlAllowOriginFilter controlAllowOriginFilter;

  @Override
  public void addInterceptors(InterceptorRegistry registry) {

    registry.addInterceptor(new AccessControlAllowOriginFilter()).addPathPatterns("/**");
  }
}
