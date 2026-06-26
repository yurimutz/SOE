package lab.window;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.KeyValue;

public class CountWindowed{
    
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);

       //Criando um "topology builder" para conseguir construir a nossa topologia
        final StreamsBuilder builder = new StreamsBuilder();

        // Serde<ArrayList<String>> arrayListSerde = 
        //         new JsonSerde<>(new TypeReference<>() {});

        KStream<String, String> source = builder.stream("user-plaintext-input");
       
        source.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
            .peek((k, v) -> System.out.println(k+":"+v))
            .groupBy((key, value) -> value)
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
            .count()
            .toStream()
            .peek((k, v) -> System.out.println(k+":"+v));
        
        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props);

        //questões de threads em java. necessário para a comunicação entre instâncias da aplicação de streams
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            //inicia a aplicação de streams, com a topologia definida
            streams.start();
            //ela fica executando, até que seja explicitamente interrompida
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
