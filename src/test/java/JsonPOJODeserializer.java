import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

class JsonPOJODeserializer<T> implements Deserializer<T> {
    private ObjectMapper mapper;

    private Class<T> tClass;

    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJODeserializer() {
        mapper = new ObjectMapper();
        mapper.registerModule(new JSR310Module());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (Class<T>) props.get("JsonPOJOClass");
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        T data;
        try {
            data = mapper.readValue(bytes, tClass);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {
    }
}
