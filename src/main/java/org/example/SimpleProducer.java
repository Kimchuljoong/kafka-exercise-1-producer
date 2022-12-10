package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {

    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    private final static String TOPIC_NAME = "test";
    private final static String MESSAGE_KEY = "Pangyo";
    private final static String BOOTSTRAP_SERVERS = "ec2-3-38-209-151.ap-northeast-2.compute.amazonaws.com:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageValue = "testMessage";
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, MESSAGE_KEY, messageValue);

        int partitionNo = 0;
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, MESSAGE_KEY, messageValue);

//        RecordMetadata metadata = producer.send(record).get();
        producer.send(record, new ProducerCallback());

//        logger.info(metadata.toString());
        logger.info("{}", record);

        producer.flush();
        producer.close();
    }
}
