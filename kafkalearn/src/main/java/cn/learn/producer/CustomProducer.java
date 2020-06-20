package cn.learn.producer;

import constant.Constance;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class CustomProducer {
    public static void main(String[] args) {

        Properties pros = new Properties();
        pros.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constance.BOOTSTRAP_SERVERS);
        pros.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        pros.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org" +
                ".apache.kafka.common.serialization.StringSerializer");

        // 有默认值的参数
        pros.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        pros.put("retries", 1);//重试次数
        pros.put("batch.size", 16384);//批次大小
        pros.put("linger.ms", 1);//等待时间
        pros.put("buffer.memory", 33554432);//RecordAccumulator 缓冲区大小


        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);

        for (int i = 0; i <10 ; i++) {
            ProducerRecord<String,String> record = new ProducerRecord<String,
                    String>("first","qiang "+i+" fiona");

            producer.send(record);
        }

        producer.close();

    }
}