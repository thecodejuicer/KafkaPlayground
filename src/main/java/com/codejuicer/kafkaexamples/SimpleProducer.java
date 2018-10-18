package com.codejuicer.kafkaexamples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * This is a very simple producer that publishes to the "test" topic. It reads input from the console
 * and publishes it to the topic until the user types "stop!" (case insensitive).
 */
public class SimpleProducer {
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        // Set the three required properties
        kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 
        KafkaProducer<String, String> producer;
        ProducerRecord<String, String> record;
        
        BufferedReader messageReader;
        
        try {
            producer = new KafkaProducer<>(kafkaProperties);
            messageReader = new BufferedReader(new InputStreamReader(System.in));
            
            String message;

            System.out.println("Send a message to the \"test\" topic. Type stop! to exit.");
            System.out.print(":> ");
            // Start an input loop. Type "stop!" to end it all.
            while(null != (message = messageReader.readLine())) {
                System.out.print(":> ");
                
                // Good bye. This is where the road ends.
                if("stop!".equals(message.toLowerCase())) {
                    System.out.println("Bye.\n");
                    break;
                }
                
                // Ignore empty strings
                if(message.trim().isEmpty()) continue;
                
                // Oh. There was a message? Instantiate a record with the message and send it.
                // This doesn't wait for any kind of response. It doesn't care. "Fire and forget"
                try {
                    record = new ProducerRecord<>("test", message);
                    producer.send(record);
                } catch(Exception e) {
                    // Simple message printing here. We don't want to terminate.
                    e.printStackTrace();
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
