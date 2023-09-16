package com.example.controller;

import com.example.dto.Customer;
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
    public ResponseEntity<?> sendMessage(@PathVariable String message) {
        try {
            for (int i = 0; i <= 100000; i++) {
                kafkaMessagePublisher.sendMessageToTopic(message + " : " + i);
            }
            return ResponseEntity.ok("Message published successfully: " + message);
        } catch (Exception e) {
            System.out.println("Not able to publish kafka message: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/publish")
    public void sendEvent(@RequestBody Customer customer) {
        try {
            kafkaMessagePublisher.sendEventsToTopic(customer);
        } catch (Exception e) {
            System.out.println("Error in sending event to topic from controller");
        }
    }
}
