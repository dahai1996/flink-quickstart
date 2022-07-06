package sink;

import function.ShuntValue;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.internal.AbstractJdbcOutputFormat;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;

/**
 * @author wfs
 */
public class ClickHouseSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    private final List<AbstractJdbcOutputFormat<T>> outputFormatList;
    private final ShuntValue<T> shuntValueImp;
    private final int size;

    public ClickHouseSinkFunction(
            List<AbstractJdbcOutputFormat<T>> outputFormatList, ShuntValue<T> shuntValueImp) {
        this.outputFormatList = outputFormatList;
        this.shuntValueImp = shuntValueImp;
        this.size = outputFormatList.size();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        for (AbstractJdbcOutputFormat<T> outputFormat : outputFormatList) {
            outputFormat.setRuntimeContext(ctx);
            outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        int x = Math.abs(shuntValueImp.shunt(value) % size);
        outputFormatList.get(x).writeRecord(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        for (AbstractJdbcOutputFormat<T> outputFormat : outputFormatList) {
            outputFormat.flush();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}
}
