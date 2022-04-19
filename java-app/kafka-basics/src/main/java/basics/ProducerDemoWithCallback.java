package basics;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static basics.DemoUtils.DEMO_TOPIC;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    /* Seting up kafka first:
        docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic demo_java --partitions 3 --replication-factor 1
        docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic demo_java --from-beginning
     */

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        log.info("Starting...");

        KafkaProducer<String, String> producer = DemoUtils.createProducer();

        // send data
        for (int i = 0; i < 10; i++) {
            // All the messages are going to the same partition because of the stick partitioner (performance optimization)
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(DEMO_TOPIC, "hello world" + UUID.randomUUID());
            Future<RecordMetadata> _future = producer.send(producerRecord, DemoUtils.getLoggerCallback());
        }

        // flush and close the produces
        producer.flush();
        producer.close();


    }


}
