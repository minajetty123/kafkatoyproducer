package org.vwb.experiment.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
@RestController
public class KafkaProducerController {

    private final KafkaProducerService kafkaProducerService;

    @Autowired
    public KafkaProducerController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping("/messages")
    public void sendMessageToKafka(@RequestBody String message) {
        kafkaProducerService.sendMessage(message);
    }
}