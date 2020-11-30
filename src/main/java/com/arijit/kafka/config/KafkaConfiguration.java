package com.arijit.kafka.config;

import com.arijit.kafka.consumer.KafkaConsumer;
import com.arijit.kafka.model.Customer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConfiguration {

    @Value("${consumer.instance}")
    private String consumerInstance;

    @Value("${retry.attempts}")
    private String retryAttempts;

    @Autowired
    private KafkaConsumer kafkaConsumer;


    @Bean
    ConcurrentKafkaListenerContainerFactory<?,?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object,Object> kafkaConsumerFactory)
    {
        Map<String, Object> config = kafkaConsumerFactory.getConfigurationProperties();
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, new DefaultKafkaConsumerFactory(config, new StringDeserializer(), new JsonDeserializer<>(Customer.class)));
        factory.setConcurrency(Integer.parseInt(consumerInstance));
        factory.setErrorHandler(((thrownException, data) -> {
            log.info("Exception is {} for data {}",thrownException.getMessage(),data);
        }));
        factory.setRetryTemplate(retryTemplate());

        factory.setRecoveryCallback(context -> {
            if(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException){
                log.info("---Recovery for Recoverable Exception---");

                //pushing to another queue as it's an recoverable exception
                Arrays.asList(context.attributeNames())
                        .forEach(attributeName -> {
                            log.info("Attribute Name is {}",attributeName);
                            log.info("Attribute value is {}",context.getAttribute(attributeName));
                        } );
                ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context.getAttribute("record");
                kafkaConsumer.handleRecovery(consumerRecord);
            }else{
                log.error("---Non Recovery for Non Recoverable Exception");
                throw new RuntimeException("Can't be recovered");
            }

            return null;
        });
        return factory;

    }

    private RetryTemplate retryTemplate(){
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(1000);
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy());
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
    }

    private RetryPolicy retryPolicy(){
        //generic retries
//        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
//        simpleRetryPolicy.setMaxAttempts(Integer.parseInt(retryAttempts));

        //custom/specific retries
        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(IllegalArgumentException.class, false);
        retryableExceptions.put(RecoverableDataAccessException.class, true);
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, retryableExceptions, true );
        return simpleRetryPolicy;
    }



}
