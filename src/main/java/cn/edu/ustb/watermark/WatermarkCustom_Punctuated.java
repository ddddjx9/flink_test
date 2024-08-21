package cn.edu.ustb.watermark;

import cn.edu.ustb.sourceOperator.WaterSensor;
import cn.edu.ustb.transformOperator.userDefinedFunction.MyMapFunction;
import cn.edu.ustb.watermark.function.MyPunctuatedGenerator;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WatermarkCustom_Punctuated {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 7777)
                .map(new MyMapFunction())
                .assignTimestampsAndWatermarks(
                        //TODO 指定自定义生成器
                        WatermarkStrategy
                                //自定义断点式水位线生成器
                                .<WaterSensor>forGenerator(context -> new MyPunctuatedGenerator(3000L)) //单位为毫)
                                .withTimestampAssigner(TimestampAssignerSupplier.of(
                                                new SerializableTimestampAssigner<WaterSensor>() {
                                                    @Override
                                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                                        return element.getTs() * 1000L;
                                                    }
                                                }
                                        )
                                )
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10L)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String windowStart= DateFormatUtils.format(start,"yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd= DateFormatUtils.format(end,"yyyy-MM-dd HH:mm:ss.SSS");
                        out.collect("key = "+s+"的窗口["+windowStart+"，"+windowEnd+"，包含了"+elements.spliterator().estimateSize()+"条数据："+elements);
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
