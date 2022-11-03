package builder;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wfs
 */
public class StreamEnvBuilder {
    private final StreamExecutionEnvironment env;

    public StreamEnvBuilder() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    public static StreamEnvBuilder builder() {
        return new StreamEnvBuilder();
    }

    /**
     * 开启并设置checkpoint
     *
     * @param checkpointInterval 毫秒,最小为10
     */
    public StreamEnvBuilder setCheckpointInterval(long checkpointInterval) {
        env.enableCheckpointing(checkpointInterval);
        return this;
    }

    /**
     * 设置一致性语义
     *
     * @param checkpointingMode 枚举类:精确一次性和至少一次性
     */
    public StreamEnvBuilder setCheckpointingMode(CheckpointingMode checkpointingMode) {
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
        return this;
    }

    /** checkpoint设置:超时时间 */
    public StreamEnvBuilder setCheckpointTimeout(long checkpointTimeout) {
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
        return this;
    }

    /** checkpoint设置:间隔时间 */
    public StreamEnvBuilder setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints) {
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
        return this;
    }

    /** checkpoint设置:失败策略 */
    public StreamEnvBuilder setTolerableCheckpointFailureNumber(
            int tolerableCheckpointFailureNumber) {
        env.getCheckpointConfig()
                .setTolerableCheckpointFailureNumber(tolerableCheckpointFailureNumber);
        return this;
    }

    /** checkpoint设置:最大同时进行checkpoint数量 */
    public StreamEnvBuilder setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);
        return this;
    }

    /** 重试策略 */
    public StreamEnvBuilder setRestartStrategy(
            RestartStrategies.RestartStrategyConfiguration restartStrategy) {
        env.setRestartStrategy(restartStrategy);
        return this;
    }

    /** 默认的重试策略基础上进行调整 */
    public StreamEnvBuilder setDefaultRestartStrategy(
            int failureRate, Time failureInterval, Time delayInterval) {
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(failureRate, failureInterval, delayInterval));
        return this;
    }

    /** 用于调整默认内存stateBackend大小 */
    public StreamEnvBuilder setHashMapStateBackend(int maxStateSizeMb) {
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig()
                .setCheckpointStorage(
                        new JobManagerCheckpointStorage(maxStateSizeMb * 1024 * 1024));
        return this;
    }

    /** 设置文件为stateBackend,并且设置路径 */
    public StreamEnvBuilder setFileBackend(String path) {
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(path));
        return this;
    }

    /** 设置并行度 */
    public StreamEnvBuilder setParallelism(int parallelism) {
        env.setParallelism(parallelism);
        return this;
    }

    public StreamExecutionEnvironment build() {
        return env;
    }
}
