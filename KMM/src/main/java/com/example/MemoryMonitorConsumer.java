package com.example; // Package declaration for the class

// Importing Kafka consumer-related classes
import org.apache.kafka.clients.consumer.ConsumerRecord;   // Represents a single record consumed from Kafka
import org.apache.kafka.clients.consumer.ConsumerRecords;  // Holds a batch of records fetched by the Kafka consumer
import org.apache.kafka.clients.consumer.KafkaConsumer;    // Kafka consumer class to poll and consume records

// Logger utility for generating log messages
import org.apache.logging.log4j.LogManager;   // Log manager for creating loggers
import org.apache.logging.log4j.Logger;       // Logger instance used for logging messages

// Java standard library imports
import java.io.FileWriter;    // Used for writing data to a file
import java.io.IOException;   // Exception handling for IO operations
import java.time.Duration;    // Represents an amount of time (used to specify poll duration)
import java.time.LocalDateTime; // Class for working with date and time

import java.util.Collections; // Utility class to work with singleton sets (used for subscribing to one topic)
import java.util.Properties;  // A class for storing and managing key-value pairs of configuration settings

/**
 * MemoryMonitorConsumer is a Kafka consumer application that listens to the "mm-usage-topic"
 * and consumes records representing the system's free memory. It logs the consumed record metadata
 * and writes the data to a text file.
 */
public class MemoryMonitorConsumer {

    // Logger instance for logging events in this class 
    private static final Logger logger = LogManager.getLogger(MemoryMonitorConsumer.class);
    public static void main(String[] args) {
        // Set Kafka properties, broker address, and group ID
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "mm-usage-group");

        // Set deserializers for the properties key/value
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Create Kafka consumer and subscribe consumer to topic mm-usage-topic
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("mm-usage-topic"));

        // Text file for logging
        String filePath = "consumer_output.txt";

        try {
            // Infinite loop to continuously poll for new records from Kafka
            while (true) {
                // Poll Kafka for new records with a timeout of 100 milliseconds
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // Iterate over the batch of consumed records
                for (ConsumerRecord<String, String> record : records) {
                    // Log the consumed record's key, value, partition, and offset
                    logger.info("Consumed record with key {} and value {}, partition {}, offset {}",
                            record.key(), record.value(), record.partition(), record.offset());

                    // Prepare a string for logging the consumed record metadata
                    String data = String.format("Consumed record with key %s and value %s, partition %s, offset %s", record.key(), record.value(), record.partition(), record.offset());
                    
                    // Get the current date and time (used for logging)
                    LocalDateTime dateTime = LocalDateTime.now();
                    String dateTimeString = dateTime.toString();

                    // Write the log entry to a text file (append to existing file)
                    try(FileWriter writer = new FileWriter(filePath, true)) {
                        writer.write(dateTimeString + " " + data);
                    } catch (IOException e) {
                        // Handle any IOExceptions that occur while writing to the file
                        e.printStackTrace();
                    }
                }
            }   
        } finally {
            // Ensure the consumer is closed when the program ends
            consumer.close();
        }
    }
}


