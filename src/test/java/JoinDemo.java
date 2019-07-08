import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class JoinDemo {
    private TopologyTestDriver driver;
    private ConsumerRecordFactory<String, String> addressFactory;
    private ConsumerRecordFactory<String, Person> personFactory;
    private StreamsBuilder builder;

    private void createPersonAddressJoiner() {
        builder = new StreamsBuilder();
        GlobalKTable<String, String> addresses = builder.globalTable("address");
        builder.stream("person", Consumed.with(Codec.of(String.class), Codec.of(Person.class)))
                .join(addresses,
                        (k, person) -> person.addressId,
                        (person, address) -> {
                            person.address = address;
                            return person;
                        })
                .to("person-enriched", Produced.with(Codec.of(String.class), Codec.of(Person.class)));
    }

    @Before
    public void setup() {
        createPersonAddressJoiner();
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-demo" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        driver = new TopologyTestDriver(builder.build(), config);
        addressFactory = new ConsumerRecordFactory<>(Codec.of(String.class).serializer(), Codec.of(String.class).serializer());
        personFactory = new ConsumerRecordFactory<>(Codec.of(String.class).serializer(), Codec.of(Person.class).serializer());
    }

    @Test
    public void shouldEnrichPersonWithAddress() {
        driver.pipeInput(addressFactory.create("address", "address_1", "123 Beach Road"));
        Person alice = new Person();
        alice.name = "alice";
        alice.addressId = "address_1";
        driver.pipeInput(personFactory.create("person", "", alice));

        ProducerRecord<String, Person> out = driver.readOutput("person-enriched", Codec.of(String.class).deserializer(), Codec.of(Person.class).deserializer());

        assertThat(out.value()).hasFieldOrPropertyWithValue("address", "123 Beach Road");
    }

    public static class Person {
        public String name;
        public String addressId;
        public String address;
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
