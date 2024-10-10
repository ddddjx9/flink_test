package cn.edu.ustb.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckpointConfigTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //这里需要观察一下Flink的webUI和HDFS的UI界面，查看相关配置和现象
        //想要存储到HDFS，需要导入Hadoop客户端相关依赖防止写入HDFS系统时出现问题，设定有权限的用户身份
        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 检查点配置
        // 启用检查点，默认是barrier对齐的，周期为5s，精准一次对齐方式
        env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE);
        // 获取检查点配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 指定检查点的存储位置
        checkpointConfig.setCheckpointStorage("hdfs://Hadoop130:8020/checkpoint");
        // checkpoint的超时时间：默认10分钟
        checkpointConfig.setCheckpointTimeout(1000*60*10);
        // 同时运行checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        // 最小等待间隔：上一轮checkpoint结束，到下一轮checkpoint开始之前，设置了>0，并发就是1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        // 指定取消作业时，checkpoint的是否还保留在外部系统
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 允许checkpoint连续失败的次数，默认为0，意思是，只要检查点保存失败，作业就挂掉
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        env.socketTextStream("localhost", 9999)
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
