package cn.edu.ustb.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class CheckpointUnalignedConfig {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        System.setProperty("HADOOP_USER_NAME", "root");

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        // 获取检查点配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 指定检查点的存储位置
        checkpointConfig.setCheckpointStorage("hdfs://Hadoop130:8020/checkpoint");
        // checkpoint的超时时间：默认10分钟
        checkpointConfig.setCheckpointTimeout(1000*60*10);
        // 同时运行checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 最小等待间隔：上一轮checkpoint结束，到下一轮checkpoint开始之前，设置了>0，并发就是1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        // 指定取消作业时，checkpoint的是否还保留在外部系统
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 允许checkpoint连续失败的次数，默认为0，意思是，只要检查点保存失败，作业就挂掉
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        //TODO 开启非barrier对齐策略
        // 开启之后，自动设置为精准一次，并发为1
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
