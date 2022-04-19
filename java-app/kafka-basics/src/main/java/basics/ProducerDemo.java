package basics;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

import static basics.DemoUtils.DEMO_TOPIC;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    /* Seting up kafka first:
        docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic demo_java --partitions 3 --replication-factor 1
        docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic demo_java --from-beginning
     */

    public static void main(String[] args) {
        log.info("Starting...");

        KafkaProducer<String, String> producer = DemoUtils.createProducer();

        // send data
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(DEMO_TOPIC, "hello world");
        Future<RecordMetadata> future = producer.send(producerRecord);

        // flush and close the produces
        producer.flush();
        producer.close();


    }
}
