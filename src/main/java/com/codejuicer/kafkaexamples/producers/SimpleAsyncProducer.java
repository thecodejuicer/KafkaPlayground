package com.codejuicer.kafkaexamples.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.StreamsConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * This is an asynchronous producer that publishes to the "test" topic. It reads input from the console
 * and publishes it to the topic until the user types "stop!" (case insensitive).
 */
public class SimpleAsyncProducer {
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        // Set the three required properties
        kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Hey look, it's a producer.
        KafkaProducer<String, String> producer;
        // This will be the record the producer sends.
        ProducerRecord<String, String> record;
        
        BufferedReader messageReader;

        /**
         * A callback for asynchronous publishing. 
         */
        class AsyncProducerCallback implements Callback {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(null != e) {
                    e.printStackTrace();
                } else {
                    System.out.println("Async result: " + recordMetadata.toString());
                }
            }
        }
        
        try {
            producer = new KafkaProducer<>(kafkaProperties);
            messageReader = new BufferedReader(new InputStreamReader(System.in));
            
            String message;

            System.out.println("Send a message to the \"test\" topic. Type stop! to exit.");
            System.out.print(":> ");
            // Start an input loop. Type "stop!" to end it all.
            while(null != (message = messageReader.readLine())) {
                
                // Good bye. This is where the road ends.
                if("stop!".equals(message.toLowerCase())) {
                    System.out.println("Bye.");
                    break;
                }
                
                // Ignore empty strings
                if(!message.trim().isEmpty()) {

                    // Oh. There was a message? Instantiate a record with the message and send it.
                    try {
                        record = new ProducerRecord<>("test", message);
                        System.out.println("Sending the message asynchronously.");
                        // Pass in the callback class for asynchronous sends.
                        producer.send(record, new AsyncProducerCallback());

                    } catch (Exception e) {
                        // Simple message printing here. We don't want to terminate.
                        e.printStackTrace();
                    }
                }

                System.out.print(":> ");
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
