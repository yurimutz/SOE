package parte3;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class StudentDeserializer implements Deserializer<Student> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Student deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            return objectMapper.readValue(data, Student.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing Student", e);
        }
    }

    @Override
    public void close() {}
}