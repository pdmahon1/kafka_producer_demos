package com.organization.first.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private final static String BOOTSTRAP_SERVER_ADDRESS = "127.0.01:9092";
    private final static String GROUP_ID = "my_test_app";
    public static void main(String[] args) {
        //step 1: create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,      BOOTSTRAP_SERVER_ADDRESS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, );

        //step 2: create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //step 3: subscribe to data
        //consumer.subscribe(Arrays.asList("first_topic", "second_topic"));
        consumer.subscribe(Collections.singleton("first_topic"));
        while(true){
            //check pom.xml, where <build> has a maven plugin for build v8
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records){
                ;
            }
        }
    }
}
