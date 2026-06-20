package lab.stream;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

public class SplitBranch {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplitfilter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

       //Criando um "topology builder" para conseguir construir a nossa topologia
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("streams-plaintext-input");

        Map<String, KStream<String, String>> branches = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")))
        .peek((key, value) -> System.out.println(key + ":" + value)) // peek pra ve se quebrou a string direito
        .split(Named.as("Branch-")) // comeca o split
        .branch((key, value) -> value.contains("kafka"), Branched.as("kafka")) // coloca palavras com kafka nessa branch
        .branch((key, value) -> value.contains("ED"), Branched.as("ED")) // coloca palavras com ED nessa branch
        .noDefaultBranch();
        
        // pega a branch kafka, da um peek pra ver se entrou mesmo e manda pro topico kafka-topic
        branches.get("Branch-kafka").peek((key, value) -> System.out.println("Caiu no kafka-topic -> Chave: " + key + " Valor: " + value)).to("kafka-topic");
        // pega a branch ED, da um peek pra ver se entrou mesmo e manda pro topico ed-topic
        branches.get("Branch-ED").peek((key, value) -> System.out.println("Caiu no ed-topic -> Chave: " + key + " Valor: " + value)).to("ed-topic");

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
