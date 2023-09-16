package com.example.service;

import com.example.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> template;

    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> send = template.send("demo-6", message);
        send.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent Message: " + message + " with offset: " + result.getRecordMetadata().offset());
            } else {
                System.out.println("Unable to send message: " + message + " due to: " + ex.getMessage());
            }
        });
    }

    public void sendEventsToTopic(Customer message) {
        try {
            CompletableFuture<SendResult<String, Object>> send = template.send("demo-7", message);
            send.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent Message: " + message + " with offset: " + result.getRecordMetadata().offset());
                } else {
                    System.out.println("Unable to send message: " + message + " due to: " + ex.getMessage());
                }
            });
        } catch (Exception e) {
            System.out.println("Exception in sending event to topic" + e.getMessage());
        }
    }
}
