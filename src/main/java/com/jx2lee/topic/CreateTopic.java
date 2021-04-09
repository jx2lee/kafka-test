package com.jx2lee.topic;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;



public class CreateTopic {

    /**
     * @param args
     * args[0]: bootstrap-server (ip:port)
     * args[1]: topic name
     */
    public static void main(String[] args) {
        final String ordersTopic = args[1];
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);

        try (final AdminClient adminClient = AdminClient.create(props)) {
            try {
                // Define topic
                NewTopic newTopic = new NewTopic(ordersTopic, 1, (short)1);

                // Create topic, which is async call.
                final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));

                // Since the call is Async, Lets wait for it to complete.
                createTopicsResult.values().get(ordersTopic).get();
            } catch (InterruptedException | ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException))
                    throw new RuntimeException(e.getMessage(), e);
            }
        }
    }
}
