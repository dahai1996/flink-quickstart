import bean.DwdOrderBean;
import bean.RunEnv;
import builder.ClickHouseSinkBuilder;
import builder.EsSinkBuilder;
import builder.SourceKafkaBuilder;
import builder.StreamEnvBuilder;
import function.ShuntValue;
import model.FlinkMainModel;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import sink.JdbcSink4ClickHouse;
import sink.SinkEs;
import sink.SinkSingleClickHouse;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author wfs
 */
public class FlinkMain extends FlinkMainModel {
    public static void main(String[] args) throws Exception {

        /*
         *   注意根据需要裁剪pom文件及相关代码
         * */

        // 环境设置
        // 1.检查是否传递配置文件路径
        ParameterTool pro = getPro(args, 0);
        // 从jar包中读取配置文件,
        ParameterTool pro2 = getProFromJar(FlinkMain.class, "/mdw-flink-quickstart.properties");

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

        // 数据源和数据目的定义
        // 1. 数据源例子: kafka source

        FlinkKafkaConsumerBase<String> sourceKafka =
                SourceKafkaBuilder.builder(uat, topicName, groupId, new SimpleStringSchema())
                        .setRequestTimeOutMs("60000")
                        .setSessionTimeOutMs("60000")
                        .build()
                        .setStartFromGroupOffsets();

        // 2. 数据目的例子: es sink
        SinkFunction<String> sinkEs =
                new SinkEs<>(
                                uat,
                                new ElasticsearchSinkFunction<String>() {
                                    @Override
                                    public void process(
                                            String s,
                                            RuntimeContext runtimeContext,
                                            RequestIndexer requestIndexer) {}
                                },
                                1,
                                5,
                                5000L)
                        .getSink();
        ArrayList<String> esHosts = new ArrayList<>(5);
        esHosts.addAll(Arrays.asList(uat.getEsHost().split(",")));
        SinkFunction<String> esSink =
                new EsSinkBuilder<>(
                                esHosts,
                                uat.getEsPort(),
                                new ElasticsearchSinkFunction<String>() {
                                    @Override
                                    public void process(
                                            String o,
                                            RuntimeContext runtimeContext,
                                            RequestIndexer requestIndexer) {}
                                })
                        .setBulkFlushMaxSizeMb(10)
                        .setBulkFlushMaxActions(1)
                        .setBulkFlushInterval(5000L)
                        .setKeepAliveDurationMinutes(15)
                        .setBulkFlushBackoff(true)
                        .setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.CONSTANT)
                        .setBulkFlushBackoffDelay(1000L)
                        .setBulkFlushBackoffRetries(3)
                        .build();

        // 3. 数据目的例子： 单节点clickhouse sink
        SinkFunction<DwdOrderBean> sinkClickhouse =
                new SinkSingleClickHouse<>(
                                "insert into tableName (id,name) values (?,?)",
                                new JdbcStatementBuilder<DwdOrderBean>() {
                                    @Override
                                    public void accept(
                                            PreparedStatement ps, DwdOrderBean dwdOrderBean)
                                            throws SQLException {
                                        Field[] fields =
                                                dwdOrderBean.getClass().getDeclaredFields();
                                        try {
                                            SinkSingleClickHouse.setPs(ps, fields, dwdOrderBean);
                                        } catch (IllegalAccessException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                },
                                uat,
                                "dwd_cdp")
                        .getSink();

        // 4. clickhouse集群，分流写入，用于分布式表
        ArrayList<String> hosts = new ArrayList<String>(3);
        hosts.add("host1");
        hosts.add("host2");
        SinkFunction<DwdOrderBean> sink =
                JdbcSink4ClickHouse.sink(
                        "insert into tableName (id,name) values (?,?)",
                        new JdbcStatementBuilder<DwdOrderBean>() {
                            @Override
                            public void accept(PreparedStatement ps, DwdOrderBean dwdOrderBean)
                                    throws SQLException {
                                Field[] fields = dwdOrderBean.getClass().getDeclaredFields();
                                try {
                                    SinkSingleClickHouse.setPs(ps, fields, dwdOrderBean);
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                }
                            }
                        },
                        new JdbcExecutionOptions.Builder()
                                .withBatchIntervalMs(5000L)
                                .withBatchSize(50000)
                                .withMaxRetries(3)
                                .build(),
                        hosts,
                        "8123",
                        "default",
                        "dddddd",
                        "defalut",
                        new ShuntValue<DwdOrderBean>() {
                            @Override
                            public int shunt(DwdOrderBean value) {
                                return value.getTenant_code().hashCode();
                            }
                        });
        // clickhouse集群，分流写入，用于分布式表
        SinkFunction<DwdOrderBean> sinkClusterClickHouse =
                ClickHouseSinkBuilder.builder(
                                "insert into tableName (id,name) values (?,?)",
                                new JdbcStatementBuilder<DwdOrderBean>() {
                                    @Override
                                    public void accept(
                                            PreparedStatement ps, DwdOrderBean dwdOrderBean)
                                            throws SQLException {
                                        Field[] fields =
                                                dwdOrderBean.getClass().getDeclaredFields();
                                        try {
                                            SinkSingleClickHouse.setPs(ps, fields, dwdOrderBean);
                                        } catch (IllegalAccessException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                },
                                hosts)
                        .setShuntValue(value -> value.getTenant_code().hashCode())
                        .setClickHouseHosts(hosts)
                        .setClickHousePassword("pas")
                        .setClickHousePort("222")
                        .setClickHouseUser("user")
                        .setClickHouseDatabase("default")
                        .setExecutionOptions(
                                new JdbcExecutionOptions.Builder()
                                        .withBatchIntervalMs(5000L)
                                        .withBatchSize(50000)
                                        .withMaxRetries(3)
                                        .build())
                        .build();

        // 转化操作
        SingleOutputStreamOperator<String> streamKafka1 =
                getKafkaSourceWithMonotonousWatermarks(
                        env, sourceKafka, Duration.ofSeconds(10), "source");
        streamKafka1.print();
        // 提交
        env.execute("name");
    }
}
