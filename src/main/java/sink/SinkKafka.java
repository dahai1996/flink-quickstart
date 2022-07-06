package sink;

import bean.RunEnv;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author wfs
 */
public class SinkKafka {
    String topic;
    int kafkaProducersPoolSize;
    Properties propertiesSink = new Properties();

    /**
     * 获取kafka sink，该sink为string类型数据提供处理，需提交做好数据转换
     *
     * @param runEnv 执行环境
     * @param requestTimeOutMs 超时时间，毫秒
     * @param topic 写入的topic名
     * @param kafkaProducersPoolSize 覆盖默认生产者池大小，默认为5
     */
    public SinkKafka(
            RunEnv runEnv, String requestTimeOutMs, String topic, int kafkaProducersPoolSize) {
        propertiesSink.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, runEnv.getKafkaHost());
        propertiesSink.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeOutMs);
        this.topic = topic;
        this.kafkaProducersPoolSize = kafkaProducersPoolSize;
    }

    /**
     * 获取kafka sink，该sink为string类型数据提供处理，需提交做好数据转换
     *
     * @param runEnv 执行环境
     * @param requestTimeOutMs 超时时间，毫秒
     * @param topic 写入的topic名
     */
    public SinkKafka(RunEnv runEnv, String requestTimeOutMs, String topic) {
        propertiesSink.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, runEnv.getKafkaHost());
        propertiesSink.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeOutMs);
        this.topic = topic;
        this.kafkaProducersPoolSize = 5;
    }

    /**
     * 获取kafka sink，使用默认序列化器
     *
     * @param kafkaSerializationSchema 指定序列化器
     * @return 返回一个kafka sink
     */
    public FlinkKafkaProducer<String> getSink(
            KafkaSerializationSchema<String> kafkaSerializationSchema) {
        return new FlinkKafkaProducer<String>(
                topic,
                kafkaSerializationSchema,
                propertiesSink,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE,
                kafkaProducersPoolSize);
    }

    /**
     * 获取kafka sink，使用默认序列化器
     *
     * @return 返回一个kafka sink
     */
    public FlinkKafkaProducer<String> getSink() {
        return new FlinkKafkaProducer<String>(
                topic,
                new DefaultKafkaSerializationSchema(topic),
                propertiesSink,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE,
                kafkaProducersPoolSize);
    }

    /**
     * 获取端到端一致性的kafka sink，使用默认序列化器
     *
     * @param transactionTimeoutMs
     *     kafka事务提交时间，应该大于checkpoint时间间隔，小于kafka设置中transaction.max.timeout.ms(默认为15分钟)
     * @return 返回一个提供事务的kafka sink
     */
    public FlinkKafkaProducer<String> getExactlyOnceSink(String transactionTimeoutMs) {
        propertiesSink.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        return new FlinkKafkaProducer<String>(
                topic,
                new DefaultKafkaSerializationSchema(topic),
                propertiesSink,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE,
                kafkaProducersPoolSize);
    }

    /**
     * 获取端到端一致性的kafka sink
     *
     * @param transactionTimeoutMs
     *     kafka事务提交时间，应该大于checkpoint时间间隔，小于kafka设置中transaction.max.timeout.ms(默认为15分钟)
     * @param kafkaSerializationSchema 指定序列化器
     * @return 返回一个提供事务的kafka sink
     */
    public FlinkKafkaProducer<String> getExactlyOnceSink(
            String transactionTimeoutMs,
            KafkaSerializationSchema<String> kafkaSerializationSchema) {
        propertiesSink.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        return new FlinkKafkaProducer<String>(
                topic,
                new DefaultKafkaSerializationSchema(topic),
                propertiesSink,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE,
                kafkaProducersPoolSize);
    }

    public static class DefaultKafkaSerializationSchema
            implements KafkaSerializationSchema<String> {
        String topic;

        public DefaultKafkaSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            return new ProducerRecord<byte[], byte[]>(
                    topic, element.getBytes(StandardCharsets.UTF_8));
        }
    }

    /** 根据cdc中source.table得名字来动态写入符合table得topic,相当于动态分流 */
    public static class SendByDatabaseAndTableKafkaSerializationSchema
            implements KafkaSerializationSchema<String> {

        private String prefix;

        public SendByDatabaseAndTableKafkaSerializationSchema(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            String table = JSON.parseObject(element).getJSONObject("source").getString("table");

            return new ProducerRecord<byte[], byte[]>(
                    prefix + table, element.getBytes(StandardCharsets.UTF_8));
        }
    }
}
