package org.vwb.experiment;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.vwb.experiment.kafka.KafkaProducerService;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@EnableScheduling
@EnableAutoConfiguration
public class App 
{
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
        String bootstrapServers = "localhost:9092";
        KafkaProducerService.TOPIC= "vwb-experiment-test-final";
        int numPartitions = 3;
        short replicationFactor = 1;

        checkAndCreateTopic(bootstrapServers, KafkaProducerService.TOPIC, numPartitions, replicationFactor);
    }

    private static void checkAndCreateTopic(String bootstrapServers, String topicName, int numPartitions, short replicationFactor) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (existingTopics.contains(topicName)) {
                System.out.println("Topic already exists.");
            } else {
                createTopic(adminClient, topicName, numPartitions, replicationFactor);
                System.out.println("Topic created successfully.");
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void createTopic(AdminClient adminClient, String topicName, int numPartitions, short replicationFactor)
            throws InterruptedException, ExecutionException {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        adminClient.createTopics(Collections.singleton(newTopic)).all().get();
    }
}
