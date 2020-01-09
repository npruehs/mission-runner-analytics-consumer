package de.npruehs.missionrunner.analytics.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class App 
{
    public static void main( String[] args )
    {
    	// Create consumer.
    	Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "missionrunnergroup");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        
        // Subscribe to topic.
        consumer.subscribe(Arrays.asList("missionrunner"));
        
        // Wait before flushing to file.
        final int eventBatchSize = 3;
        
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        
        while (true) {
        	// Poll for new records.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            
            if (buffer.size() >= eventBatchSize) {
            	// TODO(np): Flush to file.
                for (int i = 0; i < buffer.size(); ++i) {
                	System.out.println(buffer.get(i).value());
                }
            	
                consumer.commitSync();
                buffer.clear();
            }
        }
    }
}
