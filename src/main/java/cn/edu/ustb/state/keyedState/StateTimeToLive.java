package cn.edu.ustb.state.keyedState;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTimeToLive {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
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
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            ValueState<Integer> state;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                //TODO 创建StateTtlConfig
                                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                        .build();

                                //TODO 状态描述器启用Ttl
                                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("lastValueState", Types.INT);
                                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                                //TODO 通过运行时上下文获取状态，通过状态描述器定义状态
                                state = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                //先获取上一条的状态值进行打印
                                System.out.println(state != null ? state.value() : null);

                                //更新状态值，顺便查看状态生存周期
                                state.update(value.getVc());
                                out.collect(value.toString());
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
