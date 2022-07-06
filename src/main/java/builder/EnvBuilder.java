package builder;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author wfs
 */
public class EnvBuilder {
    private final ExecutionEnvironment env;

    public static EnvBuilder builder() {
        return new EnvBuilder();
    }

    public EnvBuilder() {
        env = ExecutionEnvironment.getExecutionEnvironment();
    }

    public EnvBuilder setParallelism(int parallelism) {
        env.setParallelism(parallelism);
        return this;
    }

    public ExecutionEnvironment build() {
        return env;
    }
}
