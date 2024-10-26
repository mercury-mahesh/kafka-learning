package io.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        logger.info("hello world");

        //Create the producer properties.
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String,String> producer =  new KafkaProducer<>(properties);

        //Create a producer record.
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","Hello World");

        //Send the data
        producer.send(producerRecord);

        //tell the producer to send all the data and block untill done -- synchronous
        producer.flush();

        //Flush and close the parameters
        producer.close();
    }
}
