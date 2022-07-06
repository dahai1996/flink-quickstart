package builder;

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
import sink.ClickHouseSinkFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author wfs
 */
public class ClickHouseSinkBuilder<T> {
    private final String sql;
    private final JdbcStatementBuilder<T> statementBuilder;
    private JdbcExecutionOptions executionOptions = JdbcExecutionOptions.defaults();
    private List<String> clickHouseHosts;
    private String clickHousePort = "8123";
    private String clickHouseUser = "default";
    private String clickHousePassword = "default";
    private String clickHouseDatabase = "default";
    private ShuntValue<T> shuntValue = Object::hashCode;

    private SinkFunction<T> buildAll(
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

    public static <T> ClickHouseSinkBuilder<T> builder(
            String sql, JdbcStatementBuilder<T> statementBuilder, List<String> clickHouseHosts) {
        return new ClickHouseSinkBuilder<>(sql, statementBuilder, clickHouseHosts);
    }

    public ClickHouseSinkBuilder(
            String sql, JdbcStatementBuilder<T> statementBuilder, List<String> clickHouseHosts) {
        this.sql = sql;
        this.statementBuilder = statementBuilder;
        this.clickHouseHosts = clickHouseHosts;
    }

    public ClickHouseSinkBuilder<T> setExecutionOptions(JdbcExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }

    public ClickHouseSinkBuilder<T> setClickHouseHosts(List<String> clickHouseHosts) {
        this.clickHouseHosts = clickHouseHosts;
        return this;
    }

    public ClickHouseSinkBuilder<T> setClickHousePort(String clickHousePort) {
        this.clickHousePort = clickHousePort;
        return this;
    }

    public ClickHouseSinkBuilder<T> setClickHouseUser(String clickHouseUser) {
        this.clickHouseUser = clickHouseUser;
        return this;
    }

    public ClickHouseSinkBuilder<T> setClickHousePassword(String clickHousePassword) {
        this.clickHousePassword = clickHousePassword;
        return this;
    }

    public ClickHouseSinkBuilder<T> setClickHouseDatabase(String clickHouseDatabase) {
        this.clickHouseDatabase = clickHouseDatabase;
        return this;
    }

    public ClickHouseSinkBuilder<T> setShuntValue(ShuntValue<T> shuntValue) {
        this.shuntValue = shuntValue;
        return this;
    }

    public SinkFunction<T> build() {
        return buildAll(
                sql,
                statementBuilder,
                executionOptions,
                clickHouseHosts,
                clickHousePort,
                clickHouseUser,
                clickHousePassword,
                clickHouseDatabase,
                shuntValue);
    }
}
