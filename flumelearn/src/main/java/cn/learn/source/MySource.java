package cn.learn.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends
        AbstractSource
        implements
        Configurable, PollableSource {
   private  String prefix;
   private String subfix;


    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        try {
            for (int i = 0; i <5 ; i++) {
                String info = this.prefix + "--"+i+this.subfix;
                Event event = new SimpleEvent();
                event.setBody(info.getBytes());
                getChannelProcessor().processEvent(event);
                 status = Status.READY;
            }
        } catch (Exception e) {
            status = Status.BACKOFF;
            e.printStackTrace();
        } finally {

        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
       this.prefix = context.getString("prefix");
       this.subfix = context.getString("subfix","END");
    }
}
