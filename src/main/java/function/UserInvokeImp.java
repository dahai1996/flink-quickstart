package function;

import model.SinkHbase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

/**
 * 这是样例方法
 * @author sqh
 */
public class UserInvokeImp implements SinkHbase.UserInvokeInf<String> {

    @Override
    public void invoke(BufferedMutator mutator, String value) throws IOException {

    }

    @Override
    public void invokeWithConnect(BufferedMutator mutator, String value, Connection connection) throws IOException {

    }

}
