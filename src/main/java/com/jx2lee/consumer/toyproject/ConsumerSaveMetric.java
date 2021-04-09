package com.jx2lee.consumer.toyproject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerSaveMetric {
    private static int consumerCount = 3;
    private static List<ConsumerWorker> workerThreads = new ArrayList<>();

    /**
     * @param args
     * args[0]: bootstrap-server (ip:port)
     * args[1]: group ID
     * args[2]: topic name
     */
    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, args[1]);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < consumerCount; i++) {
            ConsumerWorker consumerWorker = new ConsumerWorker(configs, args[2], i);
            workerThreads.add(consumerWorker);
            executorService.execute(consumerWorker);
        }
    }
    static class ShutdownThread extends Thread {
        public void run() {
            workerThreads.forEach(ConsumerWorker::shutdown);
            System.out.println("Bye..");
        }
    }
}
