package cn.learn.interceptor;

import constant.Constance;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class InterceptorProducer {
    public static void main(String[] args) {

        Properties pros = new Properties();
// 必须的参数
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constance.BOOTSTRAP_SERVERS);
        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org" +
                ".apache.kafka.common.serialization.StringSerializer");

        List<String> interceptors = new ArrayList<>();
        interceptors.add("cn.learn.interceptor.TimeInterceptor");
        interceptors.add("cn.learn.interceptor.CountInteceptor");

         pros.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                 interceptors);

        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord("first", "qiang->" + i + "<-fiona");

            producer.send(record, (metadata, exception) -> {
                if (exception == null){
                    System.out.println("partition = "+metadata.partition()+
                            " offset = " + metadata.offset());
                }else {
                    exception.printStackTrace();
                }

            });
        }

        producer.close();
    }
}

