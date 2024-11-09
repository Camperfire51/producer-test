package com.javatechie.service;

import com.javatechie.dto.Customer;
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
//        CompletableFuture<SendResult<String, Object>> future = template.send("javatechie-topic1", 3, null, message);
//        future.whenComplete((result, ex) -> {
//            if (ex == null) {
//                System.out.println("Sent message=[" + message +
//                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
//            } else {
//                System.out.println("Unable to send message=[" +
//                        message + "] due to: " + ex.getMessage());
//            }
//        });
        template.send("javatechie-topic1", 3, null, "hi");
        template.send("javatechie-topic1", 1, null, "hello");
        template.send("javatechie-topic1", 2, null, "welcome");
        template.send("javatechie-topic1", 2, null, "youtube");
        template.send("javatechie-topic1", 0, null, "nooooo");
    }

    public void sendEventsToTopic(Customer customer) {
        try{
            CompletableFuture<SendResult<String, Object>> future = template.send("javatechie-topic", customer);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent message=[" + customer +
                            "] with offset=[" + result.getRecordMetadata().offset() + "]");
                } else {
                    System.out.println("Unable to send message=[" +
                            customer + "] due to: " + ex.getMessage());
                }
            });
        }
        catch (Exception e){
            System.out.println("ERROR : " + e.getMessage());
        }
    }
}
