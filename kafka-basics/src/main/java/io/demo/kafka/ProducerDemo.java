package io.demo.kafka;

import org.apache.kafka.clients.producer.*;
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
        properties.setProperty("batch.size", "400");

        //Create the producer
        KafkaProducer<String,String> producer =  new KafkaProducer<>(properties);
        for (int j = 0; j <= 10; j++) {
            for (int i = 0; i <= 30; i++) {
                //Create a producer record.
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World " + i);
                //Send the data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //Executes every time a record successfully sent or an exception is thrown.
                        if (e == null) {
                            //the record was successfully sent
                            logger.info("Recieved new metadata \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error while producing ", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //tell the producer to send all the data and block untill done -- synchronous
        producer.flush();

        //Flush and close the parameters
        producer.close();
    }
}
