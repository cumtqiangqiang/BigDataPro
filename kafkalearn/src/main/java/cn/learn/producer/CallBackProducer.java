package cn.learn.producer;

import constant.Constance;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CallBackProducer {
    public static void main(String[] args) {

        Properties pros = new Properties();
// 必须的参数
        pros.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constance.BOOTSTRAP_SERVERS);
        pros.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pros.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org" +
                ".apache.kafka.common.serialization.StringSerializer");


        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String,
                    String>("first", "qiang->" + i + "<-fiona");

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
