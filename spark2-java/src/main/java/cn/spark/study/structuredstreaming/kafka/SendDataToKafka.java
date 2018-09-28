package cn.spark.study.structuredstreaming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by CTWLPC on 2018/9/28.
 */
public class SendDataToKafka {

    public static void main(String[] args) {
        SendDataToKafka sendDataToKafka = new SendDataToKafka();
        sendDataToKafka.send("mysqltest", "", "this is a test data too");
    }

    public void send(String topic, String key, String data) {
        // 要往Kafka中写入消息，需要先创建一个Producer，并设置一些属性。
        Properties props = new Properties();
        props.put("bootstrap.servers",
                "192.168.101.204:9092,192.168.101.205:9092,192.168.101.206:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 1; i < 2; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(new ProducerRecord<String, String>(topic, "" + i, data));
        }
        producer.close();
    }

}
