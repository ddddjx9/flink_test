package cn.edu.ustb.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class CheckpointChangeLog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        System.setProperty("HADOOP_USER_NAME", "root");

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.enableChangelogStateBackend(true);

        //TODO 开启非barrier对齐策略
        // 开启之后，自动设置为精准一次，并发为1
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofMinutes(2));

        env.socketTextStream("Hadoop132", 9999)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] words = s.split(" ");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
