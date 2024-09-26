package cn.edu.ustb.windowJoin;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoin {
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

        //TODO interval join
        //     1. 为了能让他们之间匹配上，需要分别做keyBy之后再进行处理，key其实就是关联条件
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(new KeySelector<Tuple3<String, Integer, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //TODO 进行间隔联结interval join
        ks1.intervalJoin(ks2)
                //TODO 指定扫描的下界和上界
                //      这里需要传递正负号，代表往前偏或者往后偏
                .between(Time.seconds(-2), Time.seconds(2))
                //TODO 设置左流的迟到数据
                //.sideOutputLeftLateData()
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * 间隔联结流的处理函数，两条流的数据匹配上才会进入此方法
                     * @param left ks1的数据
                     * @param right ks2的数据
                     * @param ctx 上下文。process函数一般都带上下文
                     * @param out 采集器
                     */
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        //进入此方法，代表的是关联上的数据，实际上在此处拿到的是inner join上的数据
                        out.collect(left + "<-------------------->" + right);
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
