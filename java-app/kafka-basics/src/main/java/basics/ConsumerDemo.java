package basics;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    /* Seting up kafka first:
        docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic demo_java --partitions 3 --replication-factor 1
        docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic demo_java --from-beginning
     */

    public static void main(String[] args) {
        log.info("Starting Consumer...");

        KafkaConsumer<String, String> consumer = DemoUtils.createConsumer();

        // poll for new data
        while (true) {
            log.info("Polling...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: {}, Value: {}, Partition: {}, Offset: {} ",
                        record.key(), record.value(), record.partition(), record.offset());
            }
        }
    }
}
