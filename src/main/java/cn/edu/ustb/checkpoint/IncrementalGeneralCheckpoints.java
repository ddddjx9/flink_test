package cn.edu.ustb.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class IncrementalGeneralCheckpoints {
    public static void main(String[] args) {
        Configuration config = new Configuration();
        //TODO 默认在flink1.15之后该功能开启
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.socketTextStream("localhost", 9999)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] words = s.split(" ");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                })
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
