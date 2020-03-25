package com.yasir.github.kafkapubsub;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreads {
    public static void main(String[] args) {
        new ConsumerWithThreads().run();

    }
    private ConsumerWithThreads()
    {
    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerWithThreads.class);
        String group_id = "my-fifth-application";
        String bootstrap_server = "localhost:9092";
        logger.info("Creating consumer Latch");
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating consumer Thread");
        Runnable consumerRunnable  = new ConsumerRunnable(
                bootstrap_server,
                group_id,
                latch);

        Thread mainThread = new Thread(consumerRunnable);
        mainThread.run();

        try {
            latch.await();
        }catch (InterruptedException e){logger.info("Closing the application");}
        finally {
            Runtime.getRuntime().addShutdownHook(new Thread( () -> {
               logger.info("Caught Shutdown hook");
            })) ;
        }

    }
    public class ConsumerRunnable implements Runnable
    {
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        public ConsumerRunnable(String bootstrap_server, String group_id, CountDownLatch latch)
        {
            this.latch = latch;
            //Creating the properties for the consumer
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_server);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            //Consumer Configurations

            //Create consumer
            this.consumer = new KafkaConsumer<String,String>(props);

            //Subscribe consumer to a topic

            // consumer.subscribe(Collections.singleton("java-topic")); // This will work for only one topic
            consumer.subscribe(Arrays.asList("java-topic"));            //This will work for more than one topics


        }

        @Override
        public void run() {
            //Collect the data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }

            }catch (WakeupException e)
            {logger.info("Received Shutdown exception");}
            finally {
                consumer.close();
                //Tell the main code that we are done

                latch.countDown();
                shutdown();
            }
        }
        public void shutdown()
        {
            //The wake up methods
            //Throws the wakeup exception

            consumer.wakeup();
        }
    }
}


