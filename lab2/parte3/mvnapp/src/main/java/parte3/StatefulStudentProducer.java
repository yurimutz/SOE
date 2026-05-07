package parte3;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class StatefulStudentProducer {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StudentSerializer.class.getName());

        KafkaProducer<String, Student> producer = new KafkaProducer<>(props);

        String topic = "stateful-student-topic";
        LocationSimulator simulator = new LocationSimulator();

        Student Alice = new Student("s1", "Alice", 20, "CS", -20.3155, -40.3128);

        try {
            while (true) {

                // 🔄 Update existing object
                simulator.updateLocation(Alice);

                ProducerRecord<String, Student> record =
                        new ProducerRecord<>(topic, Alice.getId(), Alice);

                producer.send(record);

                System.out.println("Updated & Sent: " + Alice);


                Thread.sleep(10000);
            }
        } finally {
            producer.close();
        }
    }
}