package cn.edu.ustb.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 演示无界数据流处理中的算子链
 */
public class WordCount04_UnboundedOperatorChain {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //全局禁用算子链
        //env.disableOperatorChaining();
        env.socketTextStream("localhost", 8888)
                .flatMap((FlatMapFunction<String, String>) (s, collector) -> {
                    for (String word : s.split(" ")) {
                        collector.collect(word);
                    }
                })
                .startNewChain()  //从当前节点开始开启一个新的链条
                .returns(Types.STRING)
                .map((MapFunction<String, Tuple2<String, Integer>>) value -> new Tuple2<>(value,1))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0)
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
