package cn.learn.consumer;

import constant.Constance;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {

        Properties pros = new Properties();
        pros.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constance.BOOTSTRAP_SERVERS);
//         group id
        pros.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"qqiang1");
//        自动提交offset
         pros.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
//         pros.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
//         每隔多少秒自动提交offset
         pros.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
//         重置 offset
//        两种情况会重置：1）换组 2）之前的数据不存在，consumer 保存的offset 读取不到数据
//         pros.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

         pros.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
         pros.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");



         KafkaConsumer<String,String> consumer = new KafkaConsumer(pros);

        consumer.subscribe(Arrays.asList("first"));
         while (true){
//             每隔多少秒从 broker 中拉取数据
             ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

             for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                 System.out.printf("key:%s  value:%s offset:%d\n",
                         consumerRecord.key(),
                         consumerRecord.value(),consumerRecord.offset());
             }
         }
    }
}
