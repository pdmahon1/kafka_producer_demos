package com.organization.first.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private final static String BOOTSTRAP_SERVER_ADDRESS = "127.0.01:9092";
    private static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {
        //step 1: create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      BOOTSTRAP_SERVER_ADDRESS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //step 2: create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Step 4: create Producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World!");

        //step 3: send data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null){
                    logger.error("error while producing", e);
                    return;
                }
                
                //at this point, the record was successfully sent. Do something with recordMetaData or log it
            }
        });
        
        producer.flush();
        producer.close();
    }
}
