package com.yasir.github.kafkapubsub;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(producer.class);
        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //Create producer and pass the properties
        KafkaProducer <String,String>  producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i<10;i++) {
            //Create the producer Record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("java-topic", "Hello There "  + Integer.toString(i));
            //Send the data which is producer record
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Executes when the codes successfully executes or and error is sent
                    if (e == null) {
                        logger.info("Received Metadata" + "\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing", e);
                    }


                }
            });
            producer.flush();
        }
        logger.info("Completed the script");
    }
}
