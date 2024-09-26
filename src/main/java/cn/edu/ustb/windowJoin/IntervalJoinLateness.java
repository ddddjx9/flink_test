package cn.edu.ustb.windowJoin;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class IntervalJoinLateness {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.socketTextStream(
                "localhost",9999)
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] element = value.split(" ");
                        return Tuple2.of(element[0],Integer.parseInt(element[1]));
                    }
                })
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

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.socketTextStream(
                "localhost",8888)
                .map(new MapFunction<String, Tuple3<String,Integer,Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return Tuple3.of(split[0],Integer.parseInt(split[1]),Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(3)
                                )
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple3<String, Integer, Integer>>() {
                                            @Override
                                            public long extractTimestamp(Tuple3<String, Integer, Integer> element, long recordTimestamp) {
                                                return element.f1 * 1000L;
                                            }
                                        }
                                )
                );

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

        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2)
                .between(Time.seconds(-2), Time.seconds(2))
                .sideOutputLeftLateData(new OutputTag<>("left-late-data", Types.TUPLE(Types.STRING, Types.INT)))
                .sideOutputRightLateData(new OutputTag<>("right-late-data", Types.TUPLE(Types.STRING, Types.INT, Types.INT)))
                .process(
                        new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                            @Override
                            public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                                ///进入此方法，代表的是关联上的数据，实际上在此处拿到的是inner join上的数据
                                out.collect(left + "<-------------------->" + right);
                            }
                        }
                );

        //TODO 打印输出主流中在扫描范围内匹配到的数据
        process.print();
        //TODO 打印侧流中因为迟到未被匹配上的数据
        process.getSideOutput(new OutputTag<>("left-late-data", Types.TUPLE(Types.STRING, Types.INT))).printToErr("左流迟到数据有：");
        process.getSideOutput(new OutputTag<>("right-late-data", Types.TUPLE(Types.STRING, Types.INT, Types.INT))).printToErr("右流迟到数据有：");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
