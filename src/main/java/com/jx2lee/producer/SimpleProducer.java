package com.jx2lee.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
    private static int partitionNumber = 1;

    /**
     * @param args
     * args[0]: bootstrap-server (ip:port)
     * args[1]: topic name
     */
    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        for (int index = 0; index < 10; index ++) {
            String data = "This is record: " + index;
            ProducerRecord<String, String> record = new ProducerRecord<>(args[1],
                                                      partitionNumber,
                                                      Integer.toString(index),
                                                      data);

            try {
                producer.send(record);
                System.out.println("Send to " + args[1] + " | data: " + data);
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}

