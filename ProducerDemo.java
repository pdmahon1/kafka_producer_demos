package com.organization.first.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    private final static String BOOTSTRAP_SERVER_ADDRESS = "127.0.0.1:9092";

    public static void main(String[] args) {
        //step 1: create producer properties
        Properties properties = getProperties();

        //step 2: create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Step 3: create Producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World!");

        //step 4: send data
        send(producer, record);
    }
    
    private Properties getProperties(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      BOOTSTRAP_SERVER_ADDRESS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        return properties;
    }
    
    private void send(KafkaProducer producer, ProducerRecord record){
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
