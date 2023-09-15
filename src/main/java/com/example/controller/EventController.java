package com.example.controller;

import com.example.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    private KafkaMessagePublisher kafkaMessagePublisher;

    @PostMapping("/send/{message}")
    private ResponseEntity<?> sendMessage(@PathVariable String message) {
        try {
            for (int i = 0; i <= 10000; i++) {
                kafkaMessagePublisher.sendMessageToTopic(message + " : " + i);
            }
            return ResponseEntity.ok("Message published successfully: " + message);
        } catch (Exception e) {
            System.out.println("Not able to publish kafka message: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
