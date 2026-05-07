package parte3;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StatefulStudentConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "student-state-group");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StudentDeserializer.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("stateful-student-topic"));

        StudentStateStore store = new StudentStateStore();

        try {
            while (true) {

                ConsumerRecords<String, Student> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Student> record : records) {

                    Student student = record.value();

                    // 🧠 Update state
                    store.update(student);

                    System.out.println(
                            "Received: " + student +
                                    " | partition=" + record.partition() +
                                    " | offset=" + record.offset()
                    );
                }

                // Optional: periodically print full state
                store.printState();
            }

        } finally {
            consumer.close();
        }
    }
}