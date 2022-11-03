import bean.RunEnv;
import builder.SourceKafkaBuilder;
import builder.StreamEnvBuilder;
import model.FlinkMainModel;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author wfs 更多样例见 {@link FlinkMainExample}
 */
public class FlinkMain extends FlinkMainModel {
    public static void main(String[] args) throws Exception {

        // ***************************************
        //  注意根据需要裁剪pom文件及相关代码
        // ***************************************

        // 1.参数为配置文件地址,从该地址读取配置文件
        ParameterTool pro = getPro(args, 0);

        long checkpointInterval = pro.getLong("checkpointInterval", 60000);
        int parallelism = pro.getInt("parallelism", 2);
        String topicName = pro.get("topicName", "test_oneid");
        String groupId = pro.get("topicGroupId", "test_1129");

        // 2.获取执行环境的枚举类
        RunEnv uat = getEnv("uat");
        // 3.执行环境设置
        StreamExecutionEnvironment env =
                StreamEnvBuilder.builder()
                        .setCheckpointInterval(checkpointInterval)
                        .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
                        .setCheckpointTimeout(60000L)
                        .setMinPauseBetweenCheckpoints(5000)
                        .setTolerableCheckpointFailureNumber(3)
                        .setMaxConcurrentCheckpoints(1)
                        .setDefaultRestartStrategy(
                                5, Time.of(5, TimeUnit.MINUTES), Time.of(2, TimeUnit.MINUTES))
                        .setParallelism(parallelism)
                        .build();

        // 4.创建kafka source
        FlinkKafkaConsumerBase<String> sourceKafka =
                SourceKafkaBuilder.builder(uat, topicName, groupId, new SimpleStringSchema())
                        .setRequestTimeOutMs("60000")
                        .setSessionTimeOutMs("60000")
                        .build()
                        .setStartFromGroupOffsets();

        // 5.提取kafka时间戳为水印
        SingleOutputStreamOperator<String> streamKafka1 =
                getKafkaSourceWithMonotonousWatermarks(
                        env, sourceKafka, Duration.ofSeconds(10), "source");

        // 6.设置操作:打印数据
        streamKafka1.print();
        // 7.提交,并设置作业名称
        env.execute("name");
    }
}
