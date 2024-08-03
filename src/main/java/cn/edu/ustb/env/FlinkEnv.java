package cn.edu.ustb.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkEnv {
    public static void main(String[] args) {
        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                //自动识别：是远程集群，还是idea运行环境
                //.getExecutionEnvironment();
                //这里传递配置，如果需要修改，就在new配置的时候修改conf即可
                .getExecutionEnvironment(new Configuration().set(RestOptions.BIND_PORT, "8081"));

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //TODO 2.读取数据流
        env.socketTextStream("Hadoop132", 7777)
                //TODO 3.处理数据
                //flatMap算子存在类型擦除
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    final String[] words = line.split(" ");
                    for (String word : words) {
                        final Tuple2<String, Integer> wordTuple2 = new Tuple2<>(word, 1);
                        out.collect(wordTuple2);
                    }
                })
                .setParallelism(2)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((Tuple2<String, Integer> value) -> value.f0)
                .sum(1)
                .print();

        //TODO 5.执行
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
