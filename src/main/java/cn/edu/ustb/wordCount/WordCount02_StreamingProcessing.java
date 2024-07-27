package cn.edu.ustb.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 演示流处理的写法 - 处理有界数据流
 */
public class WordCount02_StreamingProcessing {
    public static void main(String[] args) {
        //TODO 1.创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.读取数据
        final DataStreamSource<String> data = env.readTextFile("input/word.txt");

        //TODO 3.处理数据 - 切分，转换，分组，聚合
        data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        final String[] words = value.split(" ");
                        for (String word : words) {
                            final Tuple2<String, Integer> wordTuple2 = new Tuple2<>(word, 1);
                            out.collect(wordTuple2);
                        }
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                })
                // TODO 4.输出
                .print();

        //TODO 5.执行 类似于SparkStreaming jsc.start()
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
