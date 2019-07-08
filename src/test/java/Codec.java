import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class Codec {
    private static Map<Class<?>, Serde<?>> serdes;

    static {
        serdes = new HashMap<>();
        serdes.put(String.class, org.apache.kafka.common.serialization.Serdes.String());
        serdes.put(Long.class, org.apache.kafka.common.serialization.Serdes.Long());
    }

    public static <T> Serde<T> of(Class<T> type) {
        if (serdes.containsKey(type)) {
            return (Serde<T>) serdes.get(type);
        }
        JsonPOJOSerializer<T> serializer = new JsonPOJOSerializer<>();
        serializer.configure(ImmutableMap.of("JsonPOJOClass", type), false);
        JsonPOJODeserializer<T> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(ImmutableMap.of("JsonPOJOClass", type), false);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
