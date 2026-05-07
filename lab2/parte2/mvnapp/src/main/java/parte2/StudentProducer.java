package parte2;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class StudentProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Key is String, Value is Student
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StudentSerializer.class.getName());

        KafkaProducer<String, Student> producer = new KafkaProducer<>(props);

        String topic = "student-topic";

        try {
            for (int i = 0; i < 5; i++) {

                Student student = new Student(
                        "id-" + i,
                        "Student " + i,
                        20 + i,
                        "Computer Science",
                        0.0,
                        0.0
                );

                ProducerRecord<String, Student> record =
                        new ProducerRecord<>(topic, student.getId(), student);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Sent: " + student +
                                " | partition=" + metadata.partition() +
                                " offset=" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
            }

        } finally {
            producer.close();
        }
    }
}
