package cn.learn.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {
    private String prefix;
    private String subfix;

    @Override
    public Status process() throws EventDeliveryException {

        Logger logger = LoggerFactory.getLogger(MySink.class);
        Status status = null;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        txn.begin();
        try {
            Event event = channel.take();
            if (event != null){
                byte[] bytes = event.getBody();
                String info = new String(bytes);
                logger.info(this.prefix +"--" + info + "--" + this.subfix);
            }
            txn.commit();
            status = Status.READY;

        }catch (Throwable t){
            txn.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error){
                throw (Error)t;
            }
        }finally {
            txn.close();
        }


        return status;
    }

    @Override
    public void configure(Context context) {
        this.prefix = context.getString("prefix");
        this.subfix = context.getString("subfix","END");
    }
}
