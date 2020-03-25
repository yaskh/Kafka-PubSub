package com.yasir.github.kafkapubsub;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producerKeys {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(producerKeys.class);
        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //Create producer and pass the properties
        KafkaProducer <String,String>  producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i<10;i++) {
            //Create the producer Record
            String topic = "java-topic";
            String value = "Hello world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic,key,value);


            //Send the data which is producer record

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Executes when the codes successfully executes or and error is sent
                    if (e == null) {
                        logger.info(
                                "Key: " + key + "\n" +
                                "Received Metadata" + "\n" +
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
