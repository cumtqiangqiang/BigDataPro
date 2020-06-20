package cn.learn.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CountInteceptor implements ProducerInterceptor<String,String> {
    private long sucessCnt;
    private long faildCnt;
//    发送消息
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }
//发送到 broker 后的回调
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null){
            sucessCnt++;
        }else {
            faildCnt ++ ;
        }
    }

    @Override
    public void close() {
        System.out.printf("Success Count:%d ------ Faild Count:%d",sucessCnt,
                faildCnt);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
