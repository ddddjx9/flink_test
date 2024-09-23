package cn.edu.ustb.windowJoin;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 3),
                        Tuple2.of("c", 4)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(3)
                                )
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Integer>>() {

                                    @Override
                                    public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
                                        return element.f1 * 1000L;
                                    }
                                })
                );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromElements(
                        Tuple3.of("a", 1, 1),
                        Tuple3.of("a", 11, 1),
                        Tuple3.of("b", 2, 1),
                        Tuple3.of("b", 12, 1),
                        Tuple3.of("c", 14, 1),
                        Tuple3.of("d", 15, 1)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(3)
                                )
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Integer>>() {

                                    @Override
                                    public long extractTimestamp(Tuple3<String, Integer, Integer> element, long recordTimestamp) {
                                        return element.f1 * 1000L;
                                    }
                                })
                );

        ds1.join(ds2)
                //TODO ds1的keyBy
                .where(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                //TODO ds2的keyBy
                .equalTo(new KeySelector<Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * 这是一个合流处理的方法，实际上只有关联上的数据才会进行join，如果ds1和ds2没有互相匹配到，那么就不会进入此方法
                     * @param first ds1的数据
                     * @param second ds2的数据
                     * @return 返回合流之后的字符串
                     */
                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                        return first + " <----------------> " + second;
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
