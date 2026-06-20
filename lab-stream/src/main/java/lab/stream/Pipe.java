package lab.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe {

        public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

       //Criando um "topology builder" para conseguir construir a nossa topologia
        final StreamsBuilder builder = new StreamsBuilder();

        //criando um source stream, deste tópico. Ela vai receber eventos continuamente
         //os eventos são criados com (key, valor)
            //depois, a gente define um sink node da topologia, enviando os eventos para outro tópico do kafka
        builder.stream("streams-plaintext-input")
                .to("streams-pipe-output");

        //para conseguir inspecionar a topologia, podemos criar um objeto do tipo Topology
        final Topology topology = builder.build();

        //printa essa topologia construída
        System.out.println(topology.describe());

        //vamos criar agora uma aplicação kafka streams com essa topologia e propriedades
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