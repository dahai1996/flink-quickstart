package sink;

import bean.RunEnv;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

/**
 * 用于oneId更新到hbase和clickhouse两个地方
 *
 * @author sqh
 */
public class SinkHbaseAndClickHouseWithCache<IN> extends AbstractRichFunction
        implements SinkFunction<IN> {
    private static final long serialVersionUID = 1L;

    private static Connection connection;
    private static BufferedMutator mutator;
    private final int writeBufferSizeMb;
    UserInvokeInf<IN> userInvoke;
    private final String zookeeperHost;
    private final String hbaseZookeeperNodePath;
    private final String zookeeperClientPort;
    private final String sinkTableName;
    private final String username;
    private final String password;
    private final String clickhouseHost;
    private final String db;
    private final int socketTimeout = 600000;
    private ClickHouseConnection chCon;
    private ClickHouseStatement chSt;
    private final Cache<String, String> cache;
    private Table table;

    /**
     * 新建一个hbase sink
     *
     * @param runEnv 环境枚举类
     * @param sinkTableName 写入的hbase表名
     * @param userInvoke 方法参数，等价于invoke方法，该方法参数分别为： 1.mutator hbase异步缓存提交器
     */
    public SinkHbaseAndClickHouseWithCache(
            RunEnv runEnv,
            String sinkTableName,
            int writeBufferSizeMb,
            UserInvokeInf<IN> userInvoke,
            String clickhouseDb) {
        this.zookeeperHost = runEnv.getZookeeperHost();
        this.hbaseZookeeperNodePath = runEnv.getHbaseZookeeperNodePath();
        this.zookeeperClientPort = runEnv.getZookeeperClientPort();
        this.sinkTableName = sinkTableName;
        this.clickhouseHost = runEnv.getClickHouseHost();
        this.db = clickhouseDb;
        this.username = runEnv.getClickHouseUser();
        this.password = runEnv.getClickHousePassword();
        this.userInvoke = userInvoke;
        this.writeBufferSizeMb = writeBufferSizeMb;
        this.cache = CacheBuilder.newBuilder().maximumSize(50000).build();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperHost);
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, hbaseZookeeperNodePath);
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperClientPort);
        configuration.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 60000);
        configuration.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 60000);
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf("OneIdStream"));

        TableName tableName = TableName.valueOf(sinkTableName);
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        // 设置缓存1m，当达到1m时数据会自动刷到hbase
        params.writeBufferSize(1024L * 1024L * writeBufferSizeMb);
        params.listener(
                (e, mutator) -> {
                    for (int i = 0; i < e.getNumExceptions(); i++) {
                        System.out.println("Failed to send put: " + e.getRow(i));
                    }
                });

        mutator = connection.getBufferedMutator(tableName);

        // ch
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(username);
        properties.setPassword(password);
        properties.setDatabase(db);
        properties.setSocketTimeout(socketTimeout);
        ClickHouseDataSource clickHouseDataSource =
                new ClickHouseDataSource(clickhouseHost, properties);
        chCon = clickHouseDataSource.getConnection();
        chSt = chCon.createStatement();

        super.open(parameters);
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        userInvoke.invokeWithConnect(mutator, value, connection, chSt, cache, table);
    }

    @Override
    public void close() throws Exception {

        if (chSt != null) {
            chSt.close();
        }
        if (chCon != null) {
            chCon.close();
        }
        if (cache != null) {
            cache.cleanUp();
        }
        if (mutator != null) {
            mutator.flush();
            mutator.close();
        }
        if (table != null) {
            table.getName();
            table.close();
        }
        if (connection != null) {
            connection.getConfiguration();
            connection.close();
        }
        super.close();
    }

    public interface UserInvokeInf<IN> extends Serializable {

        /**
         * 针对每条数据进行的hbase提交操作
         *
         * @param mutator hbase异步缓存提交api
         * @param value 得到的单条数据
         * @param connection hbase连接器
         * @param chSt clickHouse的jdbc statement
         * @param cache cache
         * @param table table对象
         * @throws IOException 调用hbase连接执行需要抛出错误
         * @throws SQLException sql异常
         * @throws ExecutionException 异常
         */
        void invokeWithConnect(
                BufferedMutator mutator,
                IN value,
                Connection connection,
                ClickHouseStatement chSt,
                Cache<String, String> cache,
                Table table)
                throws IOException, SQLException, ExecutionException;
    }
}
