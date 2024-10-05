package com.example;  // Defines the package for the class, grouping related classes together

// Importing the Kafka producer-related classes from the Apache Kafka client library
import org.apache.kafka.clients.producer.KafkaProducer; // Kafka producer class
import org.apache.kafka.clients.producer.Producer;      // Interface for Kafka producer
import org.apache.kafka.clients.producer.ProducerConfig; // Producer configuration settings
import org.apache.kafka.clients.producer.ProducerRecord; // Represents a record/message to be sent to Kafka
import org.apache.kafka.clients.producer.RecordMetadata; // Contains metadata of the produced record

// Serializer classes for converting Java objects to byte streams for transmission
import org.apache.kafka.common.serialization.StringSerializer; // Serializes string keys and values for Kafka

// Logger utility for generating log messages
import org.apache.logging.log4j.LogManager;  // Log manager for creating loggers
import org.apache.logging.log4j.Logger;      // Logger instance used to log messages

// Java standard library imports
import java.io.FileWriter;  // FileWriter class to write text to a file
import java.io.IOException; // Handles input-output exceptions
import java.time.LocalDateTime; // Class for working with date and time in Java

import java.util.concurrent.ExecutionException; // Exception thrown when attempting to retrieve a failed async task
import java.util.Properties;  // Java class for storing and retrieving properties in key-value pairs

/**
 * MemoryMonitorProducer is a Kafka producer application that continuously monitors
 * the system's free memory and sends the data to a specified Kafka topic at regular intervals.
 * It also logs the message metadata and writes the information to a text file.
 */
public class MemoryMonitorProducer {
    
    // Logger instance for logging events in this class
    private static final Logger logger = LogManager.getLogger(MemoryMonitorProducer.class);
    public static void main(String[] args) {
        // Set Kafka topic, server config, and text file path for metadata logs
        String topicName = "mm-usage-topic";
        String bootstrapServers = "localhost:9092";
        String filePath = "producer_output.txt";

        // Properties object to configure the Kafka producer settings
        Properties props = new Properties();

        // Set Kafka broker address, serializers for key/value
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create a Kafka producer with the specified properties (key and value are both strings)
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Create infinite loop that will continuously send free memory usage to Kafka 
            while (true) {
                // Get the current free memory 
                String freeMemory = Long.toString(Runtime.getRuntime().freeMemory());

                // Create a new producer record with the topic, key, and value
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "free-memory", freeMemory);

                // Send the record to Kafka and get metadata about the message that was sent
                RecordMetadata metadata = producer.send(record).get();

                // Create a log entry with the record metadata (includes key, partition, and offset)
                String data = String.format("Record sent with key %s to partition %s with offset %s", record.key(), metadata.partition(), metadata.offset());

                // Get current date and time and convert to string (this is for logging)
                LocalDateTime dateTime = LocalDateTime.now();
                String dateTimeString = dateTime.toString();
                
                // Write the log entry to a text file (append:true) so that all logs are written to the text file
                try(FileWriter writer = new FileWriter(filePath, true)) {
                    writer.write(dateTimeString + " " + data);
                } catch (IOException e) {
                    // Hanlde any IOExceptions that occur while writing to the text file
                    e.printStackTrace();
                }
                // Additional logging for the record metadata
                logger.info("Record sent with key {} to partition {} with offset {}", record.key(), metadata.partition(), metadata.offset());

                // Wait for ten seconds before sending another record (10000 milliseconds = 10 seconds)
                Thread.sleep(10000); // Send free memory every ten seconds
            }
        } catch (ExecutionException | InterruptedException e) {
            // Log any exceptions that occur while sending records
            logger.error("Error in sending record", e);
        } finally {
            // Ensure the producer is closed when the program ends
            producer.close();
    }
  }
}

