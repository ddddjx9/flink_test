package cn.edu.ustb.wordCount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 演示流处理 - 处理无界数据流（socket数据）
 */
public class WordCount03_DealWithSocket {
    public static void main(String[] args) {
        //TODO 1.创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(env.getParallelism());

        //TODO 2.读取数据流
        final DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);

        //TODO 3.处理数据
        //Caused by: org.apache.flink.api.common.functions.InvalidTypesException: The generic type parameters of 'Collector' are missing.
        // In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved.
        // An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.FlatMapFunction' interface.
        // Otherwise the type has to be specified explicitly using type information.
        //flatMap算子存在类型擦除
        socketDS.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    final String[] words = line.split(" ");
                    for (String word : words) {
                        final Tuple2<String, Integer> wordTuple2 = new Tuple2<>(word, 1);
                        out.collect(wordTuple2);
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy((Tuple2<String, Integer> value) -> value.f0)
                .reduce((tuple1, tuple2) -> new Tuple2<>(tuple1.f0, tuple1.f1 + tuple2.f1))
                .print();

        //TODO 5.执行
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
