import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class WordCounterDemo {
    private StreamsBuilder builder;
    private TopologyTestDriver driver;
    private ConsumerRecordFactory<String, String> lineFactory;

    private void createWordCounter() {
        builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("TextLinesTopic");
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count(Materialized.as("counts-store"));
        wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(), Serdes.Long()));
    }

    @Before
    public void setup() {
        createWordCounter();
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        driver = new TopologyTestDriver(builder.build(), config);
        lineFactory = new ConsumerRecordFactory<>(Codec.of(String.class).serializer(), Codec.of(String.class).serializer());
    }

    @Test
    public void shouldCountOccurrenceOfWords() {
        driver.pipeInput(lineFactory.create("TextLinesTopic", "", "the quick brown fox jumps over the lazy dog"));
        driver.pipeInput(lineFactory.create("TextLinesTopic", "", "lazy"));

        assertNextWordCount("the", 1);
        assertNextWordCount("quick", 1);
        assertNextWordCount("brown", 1);
        assertNextWordCount("fox", 1);
        assertNextWordCount("jumps", 1);
        assertNextWordCount("over", 1);
        assertNextWordCount("the", 2);
        assertNextWordCount("lazy", 1);
        assertNextWordCount("dog", 1);
        assertNextWordCount("lazy", 2);
    }

    private void assertNextWordCount(String word, long count) {
        ProducerRecord<String, Long> out = driver.readOutput("WordsWithCountsTopic", Codec.of(String.class).deserializer(), Codec.of(Long.class).deserializer());
        assertThat(out).hasFieldOrPropertyWithValue("key", word);
        assertThat(out).hasFieldOrPropertyWithValue("value", count);
    }

    @After
    public void teardown() {
        try {
            driver.close();
        } catch (Exception e) {
            // Swallow exceptions when running on Sindows
        }
    }
}