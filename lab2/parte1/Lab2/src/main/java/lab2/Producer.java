package lab2;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "teste1";

        try {
            for (int i = 0; i < 100; i++) {
                String value = "Mensagem " + i;

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Sent to partition " + metadata.partition() +
                                " with offset " + metadata.offset());
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