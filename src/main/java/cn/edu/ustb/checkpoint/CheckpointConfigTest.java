package cn.edu.ustb.checkpoint;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CheckpointConfigTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());

        //TODO 启用检查点，周期为5秒
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //TODO 指定检查点存储位置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("checkpointData/");
        //TODO 指定检查点超时时间
        checkpointConfig.setCheckpointTimeout(1000 * 60 * 10);
        //TODO 同时运行的checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        //TODO 最小等待间隔，上一轮checkpoint结束到下一轮checkpoint开始之间的间隔，设置了>0，并发就会变成1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        //TODO 取消作业时，checkpoint的数据保留在外部系统
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //TODO 允许checkpoint连续失败的次数，默认0
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        env.socketTextStream("localhost", 7777)
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    final String[] words = line.split("checkpointData");
                    for (String word : words) {
                        final Tuple2<String, Integer> wordTuple2 = new Tuple2<>(word, 1);
                        out.collect(wordTuple2);
                    }
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                    @Override
                    public TypeInformation<Tuple2<String, Integer>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                })
                .keyBy((Tuple2<String, Integer> value) -> value.f0)
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
