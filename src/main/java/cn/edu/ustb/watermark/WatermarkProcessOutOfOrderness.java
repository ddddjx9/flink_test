package cn.edu.ustb.watermark;

import cn.edu.ustb.sourceOperator.WaterSensor;
import cn.edu.ustb.transformOperator.userDefinedFunction.MyMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WatermarkProcessOutOfOrderness {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> mainStream = env.socketTextStream("localhost", 9999)
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
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor value) throws Exception {
                        return value.getId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                //TODO 使用侧输出流对关窗后的迟到数据进行兜底处理，将迟到数据打上标签，方便后续处理。ps：第一次使用时是在算子的时候使用
                .sideOutputLateData(new OutputTag<>("late-data", Types.POJO(WaterSensor.class)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        out.collect("key = " + s + "的窗口[" + windowStart + "，" + windowEnd + "，包含了" + elements.spliterator().estimateSize() + "条数据：" + elements);
                    }
                });

        //TODO 打印主流
        mainStream.print();

        //TODO 在主流中通过标签获取侧流
        mainStream.getSideOutput(new OutputTag<>("late-data", Types.POJO(WaterSensor.class)))
                .printToErr();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
