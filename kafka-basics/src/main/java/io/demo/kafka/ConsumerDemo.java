package io.demo.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("I am a kafka consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        //Create the producer properties.
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            logger.info("Polling");
            ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record : records){
                logger.info("key: "+ record.key() +", Value" + record.value());
                logger.info("Partition: "+ record.partition() +", Offset" + record.offset());
            }
        }
    }
}
