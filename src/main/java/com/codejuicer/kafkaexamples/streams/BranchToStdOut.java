package com.codejuicer.kafkaexamples.streams;

import com.codejuicer.util.Resources;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.codejuicer.util.Resources.KAFKA_BOOTSTRAP_SERVER;

/**
 * <h1>Stream Branching</h1>
 * Example of using stream branching.<br> 
 * This trivial example branches the stream based on the first letter of the value. The values are printed to STDOUT.
 * <h2>Prerequisites</h2>
 * Kafka topic: "simple-input".<br>
 * <h3>Command line example setup</h3>
 * <h4>Create the topic:</h4>
 * <code>bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic simple-input</code>
 * <h4>Create a simple console producer</h4>
 * <code>bin/kafka-console-producer.sh --broker-list localhost:9092 --topic simple-input</code><br>
 * <br>
 * <div>Now just type stuff at the console and watch it branch.</div>
 */
public class BranchToStdOut {
    public static void main(String[] args) {
        /* Just some standard properties to make it all work. */
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "branch-output-dumper");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Resources.getInstance().getConfig(KAFKA_BOOTSTRAP_SERVER));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("simple-input");

        /* Here, the stream's branches are defined. */
        KStream<String, String>[] branches = stream.branch(
                // A branch for "a"
                (key, value) -> value.toLowerCase().startsWith("a"),
                // A branch for "b"
                (key, value) -> value.toLowerCase().startsWith("b"),
                // And a branch for everything else.
                (key, value) -> true
        );
        
        // Add a transformation for all the branches.
        branches[0].foreach((key, value) -> System.out.printf("Starts with A: %s\n", value));
        branches[1].foreach((key, value) -> System.out.printf("Starts with B: %s\n", value));
        branches[2].foreach((key, value) -> System.out.printf("Starts with something else: %s\n", value));

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }
}
