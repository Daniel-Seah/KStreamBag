import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SerdeFactoryDemo {
    @Test
    public void shouldStoreSerdesOfType() {
        SerdeFactory serdes = new SerdeFactory();
        Serde<String> stringSerde = Serdes.String();
        serdes.registerSerde(String.class, stringSerde);
        Serde<Long> longSerde = Serdes.Long();
        serdes.registerSerde(Long.class, longSerde);

        assertThat(serdes.of(String.class)).isEqualTo(stringSerde);
        assertThat(serdes.of(Long.class)).isEqualTo(longSerde);
    }

    public static class SerdeFactory {
        Map<Class<?>, Serde<?>> serdes = new HashMap<>();

        public <T> void registerSerde(Class<T> type, Serde<T> serde) {
            serdes.put(type, serde);
        }

        public <T> Serde<T> of(Class<T> type) {
            return (Serde<T>) serdes.get(type);
        }
    }
}
