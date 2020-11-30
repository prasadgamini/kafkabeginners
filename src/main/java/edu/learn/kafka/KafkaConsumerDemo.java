package edu.learn.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerDemo.class);

    public static void main(String[] args) {

        //Step 1: Create consumer config properties
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-second-app");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Step 2: Create consumer

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

        //Step 3: Subscribe to a topic(s)

        kafkaConsumer.subscribe(Arrays.asList("onv_topic"));

        //Step 4: Poll for new data
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            records.forEach(record -> LOGGER.info(String.join(" -- ",
                    "Key: " + record.key(),
                    "Value: " + record.value(),
                    record.topic(),
                    Integer.toString(record.partition()),
                    new Date(record.timestamp()).toString())
            ));


            /*

            records.forEach(record -> {
                LOGGER.info(String.join(" -- ",
                        "Key: " + record.key(),
                        "Value: " + record.value(),
                        record.topic(),
                        Integer.toString(record.partition()),
                        new Date(record.timestamp()).toString())
                );
            });
             */
        }

    }
}
