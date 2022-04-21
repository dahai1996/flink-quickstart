package sink;

import bean.RunEnv;
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

import java.io.IOException;
import java.io.Serializable;

/**
 * @author sqh
 */
public class SinkHbase<IN> extends AbstractRichFunction implements SinkFunction<IN> {
    private static final long serialVersionUID = 1L;

    /**
     * 表示使用invokeWithConnect方法
     */
    public static final boolean INVOKE_TYPE_WITH_CON = true;
    /**
     * 表示使用invoke方法
     */
    public static final boolean INVOKE_TYPE_NO_CON = false;

    private static Connection connection;
    private static BufferedMutator mutator;
    private final int writeBufferSizeMb;
    private final boolean invokeTypeWithCon;
    UserInvokeInf<IN> userInvoke;
    private final String zookeeperHost;
    private final String hbaseZookeeperNodePath;
    private final String zookeeperClientPort;
    private final String sinkTableName;


    /**
     * 新建一个hbase sink
     *  @param uat           环境枚举类
     * @param sinkTableName 写入的hbase表名
     * @param userInvoke    方法参数，等价于invoke方法，该方法参数分别为：
 *                      1.mutator hbase异步缓存提交器
     * @param invokeTypeWithCon 使用UserInvokeInf中哪个invoke方法
     */
    public SinkHbase(RunEnv uat, String sinkTableName, int writeBufferSizeMb, UserInvokeInf<IN> userInvoke, boolean invokeTypeWithCon) {
        this.zookeeperHost = uat.getZookeeperHost();
        this.hbaseZookeeperNodePath = uat.getHbaseZookeeperNodePath();
        this.zookeeperClientPort = uat.getZookeeperClientPort();
        this.sinkTableName = sinkTableName;
        this.userInvoke = userInvoke;
        this.writeBufferSizeMb = writeBufferSizeMb;
        this.invokeTypeWithCon = invokeTypeWithCon;
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


        TableName tableName = TableName.valueOf(sinkTableName);
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        params.writeBufferSize(1024L * 1024L * writeBufferSizeMb);
        params.listener((e, mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                System.out.println("Failed to send put: " + e.getRow(i));
            }
        });

        mutator = connection.getBufferedMutator(tableName);
        super.open(parameters);
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        if (invokeTypeWithCon) {
            userInvoke.invokeWithConnect(mutator, value,connection);
        }else {
            userInvoke.invoke(mutator,value);
        }
    }

    @Override
    public void close() throws Exception {
        if (mutator != null) {
            mutator.flush();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    public interface UserInvokeInf<IN> extends Serializable {
        /**
         * 针对每条数据进行的hbase提交操作
         *
         * @param mutator hbase异步缓存提交api
         * @param value   得到的单条数据
         * @throws IOException 调用hbase连接执行需要抛出错误
         */
        void invoke(BufferedMutator mutator, IN value) throws IOException;

        /**
         * 针对每条数据进行的hbase提交操作
         * @param mutator hbase异步缓存提交api
         * @param value 得到的单条数据
         * @param connection hbase连接器
         * @throws IOException 调用hbase连接执行需要抛出错误
         */
        void invokeWithConnect(BufferedMutator mutator, IN value,Connection connection) throws IOException;
    }
}
