package builder;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author sqh
 */
public class EnvBuilder {
    private final ExecutionEnvironment env;

    public EnvBuilder() {
        env = ExecutionEnvironment.getExecutionEnvironment();
    }

    public EnvBuilder setParallelism(int parallelism){
        env.setParallelism(parallelism);
        return this;
    }

    public ExecutionEnvironment build(){
        return env;
    }
}
