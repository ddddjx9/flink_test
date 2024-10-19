package cn.edu.ustb.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Savepoint {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.setProperty("HADOOP_USER_NAME","root");

        env.socketTextStream("Hadoop132", 8888)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] words = s.split(" ");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                })
                .uid("flatmap->wc")
                //.name这里的参数是指显示在Web界面上让我们看到的
                .name("wc->flatmap")
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .uid("sum->wc")
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
