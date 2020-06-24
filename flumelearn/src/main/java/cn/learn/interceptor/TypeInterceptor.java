package cn.learn.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {
    private List<Event> events ;

    @Override
    public void initialize() {
      events = new ArrayList<>();
    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String info = new String(body);

//        if (info.contains("qiang")){
//            headers.put("type","Q");
//        }else {
//            headers.put("type","F");
//        }
// kafka sink 的时候，如果 key 配置为topic，会进入不同的topic
        if (info.contains("qiang")){
            headers.put("topic","first");
        }else {
            headers.put("topic","second");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        this.events.clear();
        for (Event event: list) {
           Event interceptEvent =  this.intercept(event);
           this.events.add(interceptEvent);
        }

        return this.events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{


        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {


        }
    }
}
