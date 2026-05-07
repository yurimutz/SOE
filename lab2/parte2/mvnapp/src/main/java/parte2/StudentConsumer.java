package parte2;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StudentConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "student-group");

        // Key = String, Value = Student
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StudentDeserializer.class.getName());

        // Read from beginning if no committed offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(props);

        String topic = "student-topic";
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, Student> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Student> record : records) {

                    String key = record.key();
                    Student student = record.value();

                    System.out.println(
                            "Received Student: " + student +
                                    " | key=" + key +
                                    " | partition=" + record.partition() +
                                    " | offset=" + record.offset()
                    );
                }
            }
        } finally {
            consumer.close();
        }
    }
}