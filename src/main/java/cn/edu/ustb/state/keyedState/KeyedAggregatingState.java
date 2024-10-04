package cn.edu.ustb.state.keyedState;

import cn.edu.ustb.sourceOperator.WaterSensor;
import cn.edu.ustb.transformOperator.userDefinedFunction.MyMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedAggregatingState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 8888)
                .map(new MyMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(3)
                                )
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000L;
                                    }
                                })
                )
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    AggregatingState<Integer, Double> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        state = getRuntimeContext()
                                .getAggregatingState(
                                        new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                                "aggregateState",
                                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                                    @Override
                                                    public Tuple2<Integer, Integer> createAccumulator() {
                                                        return Tuple2.of(0, 0);
                                                    }

                                                    @Override
                                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                        //TODO 第一个为水位线的总和，第二个为每种传感器的水位线出现次数
                                                        return Tuple2.of(value + accumulator.f0, accumulator.f1 + 1);
                                                    }

                                                    @Override
                                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                        return accumulator.f0 * 1.0 / accumulator.f1;
                                                    }

                                                    @Override
                                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                                                    }
                                                },
                                                Types.TUPLE(Types.INT, Types.INT))
                                );
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        state.add(value.getVc());
                        out.collect("传感器的id为：" + value.getId() + "平均水位值为：" + state.get());
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
