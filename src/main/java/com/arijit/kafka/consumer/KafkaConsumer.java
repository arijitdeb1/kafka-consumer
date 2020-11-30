package com.arijit.kafka.consumer;

import com.arijit.kafka.model.Customer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConsumer {


    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private KafkaTemplate<Integer, Customer> kafkaTemplate;

    int i = 1;

    @KafkaListener(topics = {"${topic.name}"})
    //public void consume(ConsumerRecord<Integer, String> consumerRecord){
    public void consume(Customer customerRecord){
        log.info("Consumer Record --- "+customerRecord);
        if(customerRecord.getCustomerId()==2){
            log.info("----Retrying---"+i++);
            throw new IllegalArgumentException("2 is not a valid number");
        }

        if(customerRecord.getCustomerId()==3){
            log.info("-----Retrying Recoverable---"+i++);
            throw new RecoverableDataAccessException("This can be recovered");
        }
    }

    public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord){

        Integer key = consumerRecord.key().intValue();
        String message = consumerRecord.value();
        String topic = consumerRecord.topic();
       // kafkaTemplate.send(topic, message); //reposting the message into the topic
    }
}
