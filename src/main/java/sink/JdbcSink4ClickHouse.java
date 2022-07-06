package sink;

import function.ShuntValue;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.AbstractJdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author wfs
 */
public class JdbcSink4ClickHouse {
    public static <T> SinkFunction<T> sink(
            String sql,
            JdbcStatementBuilder<T> statementBuilder,
            JdbcExecutionOptions executionOptions,
            List<String> clickHouseHosts,
            String clickHousePort,
            String clickHouseUser,
            String clickHousePassword,
            String clickHouseDatabase,
            ShuntValue<T> shuntValue) {

        List<JdbcConnectionOptions> connectionOptionsList = new ArrayList<>(5);
        for (String clickHouseHost : clickHouseHosts) {
            JdbcConnectionOptions build =
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl(
                                    "jdbc:clickhouse://"
                                            + clickHouseHost
                                            + ":"
                                            + clickHousePort
                                            + "/"
                                            + clickHouseDatabase)
                            .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                            .withUsername(clickHouseUser)
                            .withPassword(clickHousePassword)
                            .build();
            connectionOptionsList.add(build);
        }

        List<AbstractJdbcOutputFormat<T>> outputFormatList = new ArrayList<>(5);
        for (JdbcConnectionOptions singleConnectionOption : connectionOptionsList) {
            AbstractJdbcOutputFormat<T> format =
                    new JdbcBatchingOutputFormat<>(
                            new SimpleJdbcConnectionProvider(singleConnectionOption),
                            executionOptions,
                            context -> {
                                Preconditions.checkState(
                                        !context.getExecutionConfig().isObjectReuseEnabled(),
                                        "objects can not be reused with JDBC sink function");
                                return JdbcBatchStatementExecutor.simple(
                                        sql, statementBuilder, Function.identity());
                            },
                            JdbcBatchingOutputFormat.RecordExtractor.identity());
            outputFormatList.add(format);
        }

        return new ClickHouseSinkFunction<>(outputFormatList, shuntValue);
    }
}
