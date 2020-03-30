package com.cycloneboy.bigdata.loganalysis.flume;

import com.cycloneboy.bigdata.loganalysis.flume.utils.LogUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

/** Create by sl on 2020-03-16 01:42 */
public class LogETLInterceptor implements Interceptor {

  @Override
  public void initialize() {}

  @Override
  public Event intercept(Event event) {
    byte[] body = event.getBody();
    String log = new String(body, StandardCharsets.UTF_8);

    if (log.contains("start")) {
      if (LogUtils.validateStart(log)) {
        return event;
      }
    } else {
      if (LogUtils.validateEvent(log)) {
        return event;
      }
    }

    return null;
  }

  @Override
  public List<Event> intercept(List<Event> list) {
    List<Event> interceptors = new ArrayList<>();

    for (Event event : list) {
      Event intercept = intercept(event);
      if (intercept != null) {
        interceptors.add(event);
      }
    }
    return interceptors;
  }

  @Override
  public void close() {}

  public static class Builder implements Interceptor.Builder {

    @Override
    public Interceptor build() {
      return new LogETLInterceptor();
    }

    @Override
    public void configure(Context context) {}
  }
}
