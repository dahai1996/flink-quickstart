package sink;

import bean.RunEnv;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author wfs
 */
public class SinkSingleClickHouse<T> {
    private final static String NA = "null";
    private final SinkFunction<T> sink;

    /**
     * 获取clickhouse sinkFunction
     *
     * @param sql                  插入语句，格式必须为  inert into table  a,b values (?,?)
     * @param jdbcStatementBuilder 如何用单条信息填充sql
     * @param runEnv               执行环境
     * @param database             表所在的数据库
     */
    public SinkSingleClickHouse(String sql, JdbcStatementBuilder<T> jdbcStatementBuilder,
                                RunEnv runEnv, String database) {
        sink = JdbcSink.sink(
                sql,
                jdbcStatementBuilder,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://" + runEnv.getClickHouseHost() + ":" + runEnv.getClickHousePort() + "/" + database)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername(runEnv.getClickHouseUser())
                        .withPassword(runEnv.getClickHousePassword())
                        .build()
        );
    }

    /**
     * 获取clickhouse sinkFunction
     *
     * @param sql                  插入语句，格式必须为  inert into table  a,b values (?,?)
     * @param jdbcStatementBuilder 如何用单条信息填充sql
     * @param runEnv               执行环境
     * @param database             表所在的数据库
     * @param batchIntervalMs      提交条件之：间隔
     * @param batchSize            提交条件之：数据量
     * @param maxRetries           提交重试次数
     */
    public SinkSingleClickHouse(String sql, JdbcStatementBuilder<T> jdbcStatementBuilder,
                                RunEnv runEnv, String database,
                                int batchIntervalMs, int batchSize, int maxRetries) {
        sink = JdbcSink.sink(
                sql,
                jdbcStatementBuilder,
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(batchIntervalMs)
                        .withBatchSize(batchSize)
                        .withMaxRetries(maxRetries)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://" + runEnv.getClickHouseHost() + ":" + runEnv.getClickHousePort() + "/" + database)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername(runEnv.getClickHouseUser())
                        .withPassword(runEnv.getClickHousePassword())
                        .build()
        );
    }

    public SinkFunction<T> getSink() {
        return sink;
    }

    /**
     * 用于设置clickhouse PreparedStatement的通用方法
     *
     * @param ps     PreparedStatement实例
     * @param fields 通过”实例对象.getClass().getDeclaredFields()“获得
     * @param bean   实例对象
     * @throws IllegalAccessException field.get抛出的错误
     * @throws SQLException           ps.set抛出的错误
     */
    public static void setPs(PreparedStatement ps, Field[] fields, Object bean) throws IllegalAccessException, SQLException {
        for (int i = 1; i <= fields.length; i++) {
            Field field = fields[i - 1];
            field.setAccessible(true);
            Object o = field.get(bean);
            if (o == null) {
                ps.setNull(i, 0);
                continue;
            }
            String fieldValue = o.toString();
            if (!NA.equals(fieldValue) && !"".equals(fieldValue)) {
                ps.setObject(i, fieldValue);
            } else {
                ps.setNull(i, 0);
            }
        }
    }
}
