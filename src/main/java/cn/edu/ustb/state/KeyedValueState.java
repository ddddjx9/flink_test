package cn.edu.ustb.state;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class KeyedValueState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> mainStream = env.socketTextStream("localhost", 6666)
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
                            //TODO 定义状态，此时不进行初始化
                            ValueState<Integer> state;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                //TODO 在open方法中初始化状态
                                super.open(parameters);
                                //TODO 通过运行时上下文获取状态，通过状态描述器定义状态
                                state = getRuntimeContext().getState(new ValueStateDescriptor<>("lastValueState", Types.INT));
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                //TODO 取出上一条数据的水位值
                                int lastVc = state.value() == null ? 0 : state.value();

                                //TODO 判断是否超过10，一旦大于10，输出告警
                                if (lastVc > 10 && value.getVc() > 10) {
                                    ctx.output(new OutputTag<>("WARN", Types.STRING), "连续两个水位线超过10，WARNING！！");
                                }

                                //TODO 更新自己的水位值
                                state.update(value.getVc());
                                out.collect(value.toString());
                            }
                        });

        mainStream.print();
        mainStream.getSideOutput(new OutputTag<>("WARN", Types.STRING)).printToErr("WARN");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
