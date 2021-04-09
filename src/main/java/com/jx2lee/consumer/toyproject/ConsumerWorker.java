package com.jx2lee.consumer.toyproject;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWorker implements Runnable{
    private Properties properties;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    ConsumerWorker(Properties properties, String topic, int number) {
        this.properties = properties;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        File file = new File(threadName + ".csv");

        try {
            while (true) {
                FileWriter fileWriter = new FileWriter(file, true);
                StringBuilder fileWriteBuffer = new StringBuilder();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    fileWriteBuffer.append(record.value()).append("\n");
                    System.out.println(record.value());
                }
                fileWriter.write(fileWriteBuffer.toString());
                consumer.commitSync();
                fileWriter.close();
            }
        } catch (IOException e) {
            System.out.println(threadName + " IOException" + e);
        } catch (WakeupException e ) {
            System.out.println(threadName + " WakeUpException");
        } finally {
            consumer.commitSync();
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
