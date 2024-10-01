package cn.edu.ustb.processFunction;

import cn.edu.ustb.sourceOperator.WaterSensor;
import cn.edu.ustb.transformOperator.userDefinedFunction.MyMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class ProcessFunctionSideOutput {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> mainStream = env.socketTextStream("localhost", 9999)
                .map(new MyMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(3)
                                )
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000L
                                )
                )
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) {
                        //TODO 使用侧输出流进行告警
                        if (value.getVc() > 10) {
                            ctx.output(new OutputTag<>("WARN",Types.STRING), "当前水位："+value.getVc()+"，大于阈值10!!");
                        }

                        //TODO 主流正常发送数据，这里的OUTPUT指的是主流的输出类型而不是侧流的输出类型
                        out.collect(value);
                    }
                });

        mainStream.print();
        mainStream.getSideOutput(new OutputTag<>("WARN",Types.STRING)).print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
