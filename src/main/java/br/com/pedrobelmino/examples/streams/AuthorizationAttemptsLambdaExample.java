package br.com.pedrobelmino.examples.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

@Slf4j
public class AuthorizationAttemptsLambdaExample {

    static final String INPUT_TOPIC = "authorization-attempts-input";
    static final String OUTPUT_TOPIC = "authorization-attempts-count-output";

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9094";

        final Properties streamsConfiguration = getStreamsConfiguration(bootstrapServers);

        final StreamsBuilder builder = new StreamsBuilder();
        createStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static Properties getStreamsConfiguration(final String bootstrapServers) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "authorization-attempts-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "authorization-attempts-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "c:\\temp\\kafka-state");
        return streamsConfiguration;
    }


    static void createStream(final StreamsBuilder builder) {
        builder.stream(INPUT_TOPIC)
                .peek((key, value) -> log.info("key {} value {}", key, value))
                .groupBy((key, value) -> (String)value)
                .count()
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

}
