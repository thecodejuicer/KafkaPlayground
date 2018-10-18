package com.codejuicer.kafkaexamples.producers;

import com.codejuicer.kafkaexamples.support.Widget;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * This is a very simple producer that publishes to the "widgets" topic. It reads input from the console
 * and publishes it to the topic until the user types "stop!" (case insensitive).
 */
public class WidgetProducer {
    static String getInput(String prompt) throws IOException {
        BufferedReader messageReader = new BufferedReader(new InputStreamReader(System.in));
        System.out.print(prompt);
        String input = messageReader.readLine();

        if("stop!".equals(input.toLowerCase())) {
            System.out.println("Bye.\n");
            System.exit(0);
        }
        
        return input;
    }
    
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        // Set the three required properties
        kafkaProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "com.codejuicer.kafkaexamples.serializers.WidgetSerializer");

        // 
        KafkaProducer<String, Widget> producer;
        ProducerRecord<String, Widget> record;
        
        try {
            producer = new KafkaProducer<>(kafkaProperties);
            
            String message;
            boolean readmore = true;
            int widgetId = 1;

            System.out.println("Send a Widget to the \"test\" topic. Type stop! to exit.");
            // Start an input loop. Type "stop!" to end it all.
            while(readmore) {
                // The input isn't checked for empty strings. I just didn't feel like writing that for the example.
                String widgetName = getInput("Widget name :> ");
                String widgetDescription = getInput("Widget description :> ");
                
                // Oh. There was a message? Instantiate a record with the message and send it.
                // This doesn't wait for any kind of response. It doesn't care. "Fire and forget"
                try {
                    record = new ProducerRecord<>("widgets", new Widget(widgetId++, widgetName, widgetDescription));
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
