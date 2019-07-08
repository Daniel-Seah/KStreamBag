import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;

import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class TopoCleanUpDemo {
    @Test
    public void shouldFailToCleanUpRocksDB_whenRunOnWindows() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topo-clean-up-demo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        StreamsBuilder builder = new StreamsBuilder();
        builder.table("topic");
        TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config);

        Throwable error = catchThrowable(() -> driver.close());

        assertThat(error).isNotNull();
    }

    @Test
    public void swallowExceptionAndRandomiseTestId() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topo-clean-up-demo-" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        StreamsBuilder builder = new StreamsBuilder();
        builder.table("topic");
        TopologyTestDriver driver = new TopologyTestDriver(builder.build(), config);

        try {
            driver.close();
        } catch (Exception e) {
            // Swallow exception for windows. Remember to cleanup /tmp directory periodically
        }
    }
}
