package com.alex.gmall.interceptor;

import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {
    Gson gson=null;

    @Override
    public void initialize() {

        gson= new Gson();

    }

    @Override
    public Event intercept(Event event) {
        String logstr = new String(event.getBody());

        HashMap logMap = gson.fromJson(logstr, HashMap.class);

        String type = (String) logMap.get("type");

        Map<String, String> headers = event.getHeaders();
        headers.put("type",type);


        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        for (Event event : list) {

            intercept(event);

        }

        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


}
