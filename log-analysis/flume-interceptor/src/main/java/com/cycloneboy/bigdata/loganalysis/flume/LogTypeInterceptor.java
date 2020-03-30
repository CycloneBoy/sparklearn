package com.cycloneboy.bigdata.loganalysis.flume;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

/** Create by sl on 2020-03-16 10:04 */
public class LogTypeInterceptor implements Interceptor {

  @Override
  public void initialize() {}

  @Override
  public Event intercept(Event event) {
    byte[] body = event.getBody();
    String log = new String(body, StandardCharsets.UTF_8);

    Map<String, String> headers = event.getHeaders();
    if (log.contains("start")) {
      headers.put("topic", "topic_start");
    } else {
      headers.put("topic", "topic_event");
    }

    return event;
  }

  @Override
  public List<Event> intercept(List<Event> list) {
    List<Event> logEvents = new ArrayList<>();

    for (Event event : list) {
      Event intercept = intercept((event));
      logEvents.add(intercept);
    }
    return logEvents;
  }

  @Override
  public void close() {}

  public static class Builder implements Interceptor.Builder {

    @Override
    public Interceptor build() {
      return new LogTypeInterceptor();
    }

    @Override
    public void configure(Context context) {}
  }
}
