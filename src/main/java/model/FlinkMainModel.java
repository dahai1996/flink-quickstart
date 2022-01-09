package model;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * flink主函数模板
 *
 * @author sqh
 */
public class FlinkMainModel {
    public static final String ENV_XDP = "xdp";


    /**
     * 以第一个参数作为配置文件地址，读取该配置文件，没有配置文件将退出程序
     *
     * @param args 主函数参数
     * @return 配置文件工具
     */
    public static ParameterTool getPro(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("pleas add properties path as args[0] !!!");
            return null;
        }
        String propertiesPath = args[0];

        return ParameterTool.fromPropertiesFile(propertiesPath);
    }

    /**
     * 以指定序号参数作为配置文件地址，读取该配置文件，没有配置文件将退出程序
     *
     * @param args 主函数参数
     * @param pos 指定参数序号为配置文件地址
     * @return 配置文件工具
     */
    public static ParameterTool getPro(String[] args, int pos) throws IOException {
        if (pos < 0 || args.length < pos+1) {
            System.out.println("main args error!!!");
            return null;
        }
        String propertiesPath = args[pos];

        return ParameterTool.fromPropertiesFile(propertiesPath);
    }


    /**
     * 获取环境地址，包括：kafka，es，mysql
     *
     * @param mode uat或者xdp
     * @return 一个枚举类，包含了各个系统的地址
     */
    public static RunEnv getEnv(String mode) {
        if (ENV_XDP.equals(mode)) {
            return RunEnv.xdp;
        } else {
            return RunEnv.uat;
        }
    }


    /**
     * @param env         流执行环境
     * @param sourceKafka kafka数据源
     * @param duration    水印空闲时间
     * @param name        该步骤name
     * @return 一个带水印的kafka数据源，水印来自于kafka自带的时间戳
     */
    public static SingleOutputStreamOperator<String> getKafkaSourceWithMonotonousWatermarks(StreamExecutionEnvironment env, FlinkKafkaConsumerBase<String> sourceKafka, Duration duration, String name) {
        return env.addSource(sourceKafka).assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withIdleness(duration)).name(name);
    }

}
