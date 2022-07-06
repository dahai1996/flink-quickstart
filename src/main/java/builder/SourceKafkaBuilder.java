package builder;

import bean.RunEnv;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author wfs
 */
public class SourceKafkaBuilder<T> {
    private final Properties properties = new Properties();
    private final String topic;
    private final DeserializationSchema<T> valueDeserializer;

    public static <T> SourceKafkaBuilder<T> builder(
            RunEnv runEnv,
            String topic,
            String groupId,
            DeserializationSchema<T> valueDeserializer) {
        checkNotNull(runEnv);
        checkNotNull(topic);
        checkNotNull(groupId);
        return new SourceKafkaBuilder<>(runEnv, topic, groupId, valueDeserializer);
    }

    public SourceKafkaBuilder(
            RunEnv runEnv,
            String topic,
            String groupId,
            DeserializationSchema<T> valueDeserializer) {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, runEnv.getKafkaHost());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        this.topic = topic;
        this.valueDeserializer = valueDeserializer;
    }

    public SourceKafkaBuilder<T> setSessionTimeOutMs(String sessionTimeOutMs) {
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeOutMs);
        return this;
    }

    public SourceKafkaBuilder<T> setRequestTimeOutMs(String requestTimeOutMs) {
        properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeOutMs);
        return this;
    }

    public SourceKafkaBuilder<T> setAutoOffsetResetConfig(String autoOffsetResetConfig) {
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
        return this;
    }

    public SourceKafkaBuilder<T> setExactlyOnce() {
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return this;
    }

    /**
     * @param key ConsumerConfig中包含的key
     * @param value 值
     * @return 构造器
     * @see ConsumerConfig
     */
    public SourceKafkaBuilder<T> setPropertyValue(String key, String value) {
        properties.setProperty(key, value);
        return this;
    }

    public FlinkKafkaConsumer<T> build() {
        return new FlinkKafkaConsumer<>(topic, valueDeserializer, properties);
    }
}
