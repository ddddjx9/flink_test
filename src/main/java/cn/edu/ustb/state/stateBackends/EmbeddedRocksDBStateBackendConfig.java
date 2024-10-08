package cn.edu.ustb.state.stateBackends;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EmbeddedRocksDBStateBackendConfig {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
    }
}
