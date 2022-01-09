import bean.DwdOrderBean;
import builder.SourceKafkaBuilder;
import builder.StreamEnvBuilder;
import model.FlinkMainModel;
import model.RunEnv;
import model.SinkClickHouse;
import model.SinkEs;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;


/**
 * @author sqh
 */
public class FlinkMain extends FlinkMainModel {
    public static void main(String[] args) throws Exception {

        /*
         *   注意根据需要裁剪pom文件及相关代码
         * */

        //环境设置
        //1.是否需要判定配置文件
        ParameterTool pro = getPro(args,0);
        if (pro == null) {
            System.out.println("properties file is null,exit !");
            return;
        }
        long checkpointInterval = pro.getLong("checkpointInterval", 60000);
        int parallelism = pro.getInt("parallelism", 2);
        String topicName = pro.get("topicName", "test_oneid");
        String groupId = pro.get("topicGroupId", "test_1129");

        //2.获取执行环境的枚举类
        //我司有两套环境：uat，xdp，所以以此作为后缀
        RunEnv uat = getEnv("uat");
        //3.执行环境设置


        StreamExecutionEnvironment env = new StreamEnvBuilder()
                .setCheckpointInterval(checkpointInterval)
                .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
                .setCheckpointTimeout(6000L)
                .setMinPauseBetweenCheckpoints(5000)
                .setMaxConcurrentCheckpoints(1)
                .setDefaultRestartStrategy(5, Time.of(5, TimeUnit.MINUTES), Time.of(2, TimeUnit.MINUTES))
                .setParallelism(parallelism)
                .setHashMapStateBackend(25)
                .build();


        // 数据源和数据目的定义
        //1. 数据源例子: kafka source

        FlinkKafkaConsumerBase<String> sourceKafka = new SourceKafkaBuilder<>(uat, topicName, groupId, new SimpleStringSchema())
                .setRequestTimeOutMs("6000")
                .setSessionTimeOutMs("6000")
                .build()
                .setStartFromTimestamp(1635907562000L);


        //2. 数据目的例子: es sink
        SinkFunction<String> sinkEs = new SinkEs<>(
                uat,
                new ElasticsearchSinkFunction<String>() {
                    @Override
                    public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

                    }
                },
                1,
                5,
                5000L)
                .getSink();

        //3. 数据目的例子： clickhouse sink
        SinkFunction<DwdOrderBean> sinkClickhouse = new SinkClickHouse<>("insert into tableName (id,name) values (?,?)",
                new JdbcStatementBuilder<DwdOrderBean>() {
                    @Override
                    public void accept(PreparedStatement ps, DwdOrderBean dwdOrderBean) throws SQLException {
                        Field[] fields = dwdOrderBean.getClass().getDeclaredFields();
                        try {
                            SinkClickHouse.setPs(ps, fields, dwdOrderBean);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },
                uat,
                "dwd_cdp")
                .getSink();


        //转化操作
        SingleOutputStreamOperator<String> streamKafka1 = getKafkaSourceWithMonotonousWatermarks(env, sourceKafka, Duration.ofSeconds(10), "source");
        streamKafka1.print();
        //提交
        env.execute();
    }

}
