package basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

public class DemoUtils {

    public static final String DEMO_TOPIC = "demo_java";
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private static final Logger log = LoggerFactory.getLogger(DemoUtils.class);


    public static KafkaProducer<String, String> createProducer() {
        // create producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }

    public static KafkaConsumer<String, String> createConsumer() {
        // create consumer Properties
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create the consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topics
        kafkaConsumer.subscribe(Collections.singletonList(DEMO_TOPIC));

        return kafkaConsumer;
    }

    public static Callback getLoggerCallback() {
        Callback loggerCallback = (metadata, e) -> {
            // executes every time a record is successfully sent or an e is thrown
            if (Objects.nonNull(e)) {
                log.error("Error while sending message", e);
                return;
            }

            log.info("\n####### Received new metadata\n" +
                            "\tTopic {}\n" +
                            "\tPartition {}\n" +
                            "\tOffset {}\n" +
                            "\ttimestamp {}\n",
                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
        };
        return loggerCallback;
    }

}
