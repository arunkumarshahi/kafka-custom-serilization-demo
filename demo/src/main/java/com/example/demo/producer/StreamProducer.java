package com.example.demo.producer;

import com.example.demo.model.Greeting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Calendar;

@Configuration
public class StreamProducer {
    @Autowired
    private KafkaTemplate<String, Greeting> kafkaTemplate;
    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;
    @Value(value = "${message.topic.name}")
    private String topicName;

    @Scheduled(initialDelay = 1000, fixedRate = 1000)
    public void run() {
        sendMessage(Greeting.builder().text("Current time is :: " + Calendar.getInstance().getTime()).build(), topicName);
    }

    public void sendMessage(Greeting message, String topicName) {

        ListenableFuture<SendResult<String, Greeting>> future =
                kafkaTemplate.send(topicName, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, Greeting>>() {

            @Override
            public void onSuccess(SendResult<String, Greeting> result) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=["
                        + message + "] due to : " + ex.getMessage());
            }
        });
    }
}
