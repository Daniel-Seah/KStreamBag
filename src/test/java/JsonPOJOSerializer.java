import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

class JsonPOJOSerializer<T> implements Serializer<T> {
    private final ObjectMapper mapper;

    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJOSerializer() {
        mapper = new ObjectMapper();
        mapper.registerModule(new JSR310Module());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;

        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }
}
