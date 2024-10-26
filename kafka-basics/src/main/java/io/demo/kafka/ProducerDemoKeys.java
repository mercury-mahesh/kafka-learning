package io.demo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("hello world");

        //Create the producer properties.
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0;j<2;j++) {
            for (int i = 0; i <10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world" + i;
                //Create a producer record.
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                //Send the data
                producer.send(producerRecord, (recordMetadata, e) -> {
                    //Executes every time a record successfully sent or an exception is thrown.
                    if (e == null) {
                        //the record was successfully sent
                        logger.info(
                                "Key: " + key + "|" +
                                        "Partition: " + recordMetadata.partition()
                        );
                    } else {
                        logger.error("Error while producing ", e);
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
