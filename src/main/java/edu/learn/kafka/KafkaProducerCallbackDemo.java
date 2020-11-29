package edu.learn.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

public class KafkaProducerCallbackDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerCallbackDemo.class);

    public static void main(String[] args) {
        LOGGER.info("Om Namo Venkatesaya");

        //Step 1: Create Producer properties

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Step 2: Create the Producer


        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties)) {
            //Step 3: Send data
            System.out.println("Producing data");
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("onv_topic", "k-" + (i % 20), "v-" + i);
                kafkaProducer.send(record, (metadata, exception) -> {
                    LOGGER.info(String.join(" -- ",
                                    metadata.topic(),
                                    Integer.toString(metadata.partition()),
                                    new Date(metadata.timestamp()).toString(),
                                    Long.toString(metadata.offset())));
                });
            }
            kafkaProducer.flush();
        }

        LOGGER.info(".Done.");
    }
}
