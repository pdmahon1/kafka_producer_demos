package com.github.twitterAPI;

/*
 * This file uses the Twitter API and an external library to connect to the client and
 * produce requests and consume messages in the form of Tweets
 *
 * How this works:
 * - The run() method is called
 * - Establish connection to the client (in this case, the Twitter API)
 * -
 */

//you can ignore practically all of these inputs since they relate to Twitter API development

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

//these are specific to kafka logging
//you need these for creating the Producer and Consumer,
//and for connecting to the Client

public class TwitterProducerNoKeys {
    //these keys are used for connecting to the client.
    //I deleted the keys since they belonged to my personal Twitter Dev account
    private final String consumerKey = "";
    private final String consumerSecret = "";
    private final String token = "";
    private final String tokenSecret = "";

    //used for connecting to the local Kafka server
    private final String BOOTSTRAP_SERVER_ADDRESS = "127.0.0.1:9092";

    //not needed for this, but it's nice to have
    private Logger logger = LoggerFactory.getLogger(TwitterProducerNoKeys.class.getName());

    /** Constructor */
    public TwitterProducerNoKeys() {  ;  } //constructor not needed

    public static void main(String[] args) {
        new TwitterProducerNoKeys().run(); //starts the producer and consumer processes
    }

    /**
     * Creates connection with the Twitter client, polls and retrieves individual
     * Tweets, and adds them to our local Kafka server as a Producer
     */
    public void run(){
        logger.info("Setup");

        //the client will use the msgQueue to add messages (Tweets) for consuming
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String> producer = createKafkaProducer();

        String msg;
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            msg = null;
            try {
                // this is retrieving the Tweets (essentially a Twitter consumer)
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if(msg != null){
                logger.info(msg);
                //adds the Produced message to our local Kafka server
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            logger.error("Something bad happened :(");
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }

    /**
     * Creates the connection to Twitter using a 3rd party Java library. We won't be using
     * that library for USAA work.
     */
    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("trump");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        //I deleted these values and left blank Strings because they are specific to my person
        //Twitter dev account. So :P that
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        //Create a client, will differ from USAA standards
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    /**
     * Builds and returns the Producer that will add Tweets into our topic
     */
    private KafkaProducer createKafkaProducer(){
        //this is standard Producer-building from the Apache documentation
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_ADDRESS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }
}
