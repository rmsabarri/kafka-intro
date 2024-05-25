package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producerdemo {

    private static final Logger log= LoggerFactory.getLogger(Producerdemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting Producerdemo");

        //create prod properties
        Properties properties = new Properties();
       //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set prod properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create prod
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create prod record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>( "demo_java", "hello world");

        //send data
        producer.send(producerRecord);

        //flush - send all data and block untill done
        producer.flush();
        //close - flush is auto
        producer.close();



    }
}
