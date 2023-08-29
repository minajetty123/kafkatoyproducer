package org.vwb.experiment.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    public static String TOPIC = "";

    @Autowired
    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        Random random = new Random();
        int randomUserId = random.nextInt(3);
        kafkaTemplate.send(TOPIC, randomUserId, "user_id_"+String.valueOf(randomUserId), message);
        System.out.println("Message sent: " + message+" partition is: "+randomUserId);
    }
}