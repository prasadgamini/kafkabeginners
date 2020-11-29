package edu.learn.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {

    public static void main(String[] args) {
        System.out.println("Om Namo Venkatesaya");

        //Step 1: Create Producer properties

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Step 2: Create the Producer

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties);

        //Step 3: Send data
        System.out.println("Producing data");
        for (int i = 0; i < 15; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("onv_topic", "k-" + (i % 3), "v-" + i);
            kafkaProducer.send(record);
        }


        kafkaProducer.flush();
        kafkaProducer.close();

        System.out.println(".Done.");
    }
}
