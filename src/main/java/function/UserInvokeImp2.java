package function;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import ru.yandex.clickhouse.ClickHouseStatement;
import sink.SinkHbaseAndClickHouse;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.UUID;

/**
 * @author sqh
 */
public class UserInvokeImp2 implements SinkHbaseAndClickHouse.UserInvokeInf<String> {
    final byte[] family = Bytes.toBytes("map");
    final byte[] qualifierKey = Bytes.toBytes("key");
    final byte[] qualifierOneId = Bytes.toBytes("oneId");

    @Override
    public void invoke(BufferedMutator mutator, String value) throws IOException {}

    @Override
    public void invokeWithConnect(
            BufferedMutator mutator, String value, Connection connection, ClickHouseStatement cHSt)
            throws IOException, SQLException {
        // clickhouse执行与hbase同样的操作
        // 但是查询通过hbase完成，

        String[] split = value.split("\\|");

        Table table = connection.getTable(TableName.valueOf("OneIdStream"));

        Get get1 = new Get(Bytes.toBytes(split[0]));
        if (split.length == 1) {
            // 就一个信息,边就一个点
            Result result1 = table.get(get1);
            byte[] oneId1 = result1.getValue(family, qualifierOneId);
            String SOneId1 = Bytes.toString(oneId1);

            if (SOneId1 == null) {
                UUID newOneId = UUID.nameUUIDFromBytes(Bytes.toBytes(split[0]));
                Put put1 = new Put(Bytes.toBytes(split[0]));
                put1.addColumn(family, qualifierKey, Bytes.toBytes(split[0]));
                put1.addColumn(family, qualifierOneId, getBytesFromUuid(newOneId));
                mutator.mutate(put1);
                mutator.flush();
                // ch
                cHSt.addBatch(getCHSqlString(split[0], newOneId.toString()));
                cHSt.executeBatch();
            }

        } else {
            // 一个完整的边关系
            Get get2 = new Get(Bytes.toBytes(split[1]));
            Result result1 = table.get(get1);
            Result result2 = table.get(get2);
            byte[] oneId1 = result1.getValue(family, qualifierOneId);
            byte[] oneId2 = result2.getValue(family, qualifierOneId);

            String SOneId1 = Bytes.toString(oneId1);
            String SOneId2 = Bytes.toString(oneId2);

            // 通过本条边两个点 对应的oneId结果判断,后续走不同的处理
            // 先判断是否取到空值
            if (SOneId1 == null && SOneId2 != null) {
                // 1为空，2不为空，此时插入  （row1，oneId2）
                Put put = new Put(Bytes.toBytes(split[0]));
                put.addColumn(family, qualifierKey, Bytes.toBytes(split[0]));
                put.addColumn(family, qualifierOneId, oneId2);
                mutator.mutate(put);
                // ch
                cHSt.addBatch(getCHSqlString(split[0], SOneId2));

            } else if (SOneId1 != null && SOneId2 == null) {
                // 1不为空，2为空，此时插入  （row2，oneId1）
                Put put = new Put(Bytes.toBytes(split[1]));
                put.addColumn(family, qualifierKey, Bytes.toBytes(split[1]));
                put.addColumn(family, qualifierOneId, oneId1);
                mutator.mutate(put);
                // ch
                cHSt.addBatch(getCHSqlString(split[1], SOneId1));
            } else if (SOneId1 == null) {
                // 两个oneId都为空，插入两条，（row1,新oneid1）（row2，新oneid1）
                UUID newOneId = UUID.nameUUIDFromBytes(Bytes.toBytes(split[0]));
                Put put1 = new Put(Bytes.toBytes(split[0]));
                put1.addColumn(family, qualifierKey, Bytes.toBytes(split[0]));
                put1.addColumn(family, qualifierOneId, getBytesFromUuid(newOneId));
                Put put2 = new Put(Bytes.toBytes(split[1]));
                put2.addColumn(family, qualifierKey, Bytes.toBytes(split[1]));
                put2.addColumn(family, qualifierOneId, getBytesFromUuid(newOneId));

                mutator.mutate(put1);
                mutator.mutate(put2);
                // ch
                cHSt.addBatch(getCHSqlString(split[0], newOneId.toString()));
                cHSt.addBatch(getCHSqlString(split[1], newOneId.toString()));
            } else if (!SOneId1.equals(SOneId2)) {
                // 两个oneid为不同的值，此时需要融合为同一个oneid
                // 1.获取oneid1的所有行，oneid2的所有行 ，得到行键list

                ArrayList<byte[]> keyList = new ArrayList<>(4);
                SingleColumnValueFilter filter1 =
                        new SingleColumnValueFilter(
                                family, qualifierOneId, CompareFilter.CompareOp.EQUAL, oneId1);
                SingleColumnValueFilter filter2 =
                        new SingleColumnValueFilter(
                                family, qualifierOneId, CompareFilter.CompareOp.EQUAL, oneId2);
                Scan scan1 = new Scan().setFilter(filter1);
                Scan scan2 = new Scan().setFilter(filter2);
                ResultScanner results1 = table.getScanner(scan1);
                ResultScanner results2 = table.getScanner(scan2);
                for (Result result : results1) {
                    keyList.add(result.getValue(family, qualifierKey));
                }
                for (Result result : results2) {
                    keyList.add(result.getValue(family, qualifierKey));
                }

                // 2.更新所有行键的oneId字段为同一个oneId
                for (byte[] bytes : keyList) {
                    Put put = new Put(bytes);
                    put.addColumn(family, qualifierOneId, oneId1);
                    mutator.mutate(put);
                    // ch
                    cHSt.addBatch(getCHUpdateSqlString(oneId1, bytes));
                }
            }
            mutator.flush();
            // ch
            cHSt.executeBatch();
        }
    }

    private String getCHSqlString(String key, String oneId) {
        return "insert into OneIdStream (key,oneId,tenant_code) values ('"
                + key
                + "','"
                + oneId
                + "','"
                + key.split("-")[0]
                + "');";
    }

    private String getCHUpdateSqlString(byte[] oneId1, byte[] key) {
        return "ALTER TABLE OneIdStream update oneId='"
                + Bytes.toString(oneId1)
                + "' where key='"
                + Bytes.toString(key)
                + "';";
    }

    private byte[] getBytesFromUuid(UUID uuid) {
        return Bytes.toBytes(uuid.toString());
    }
}
