package cn.edu.ustb.state.keyedState;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

public class KeyedMapState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 8888)
                .map((MapFunction<String, WaterSensor>) value -> {
                    String[] split = value.split(" ");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
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
                            MapState<Integer, Integer> state;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                state = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", Types.INT, Types.INT));
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                if (!state.contains(value.getVc())) {
                                    // 如果当前计数为null，说明该键之前没有被添加。
                                    state.put(value.getVc(), 1);
                                } else {
                                    // 如果该键已经存在，则计数加一
                                    state.put(value.getVc(), state.get(value.getVc()) + 1);
                                }

                                // 构造输出结果
                                StringBuilder sb = new StringBuilder();
                                sb.append("传感器id为：").append(value.getId()).append("\n");
                                Iterable<Map.Entry<Integer, Integer>> entries = state.entries();
                                for (Map.Entry<Integer, Integer> entry : entries) {
                                    sb.append(entry.getKey()).append("出现的次数为：").append(entry.getValue()).append("\n");
                                }

                                out.collect(sb.toString());
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
