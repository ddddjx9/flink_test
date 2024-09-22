package cn.edu.ustb.watermark;

import cn.edu.ustb.transformOperator.partitionOperator.MyPartitioner;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkIdleWaiting {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<Integer> sensorDS = env
                .socketTextStream("localhost", 9999)
                .partitionCustom(
                        new MyPartitioner(),
                        new KeySelector<String, String>() {
                            @Override
                            public String getKey(String value) throws Exception {
                                return value;
                            }
                        })
                .map((MapFunction<String, Integer>) Integer::parseInt)
                //TODO 自定义分区器，数据 % 分区数量
                //      如果只输入奇数，那么就只会去往map的一个子任务
                //      奇数一组，偶数一组，开10s的事件时间滚动窗口
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Integer>() {
                                    @Override
                                    public long extractTimestamp(Integer element, long recordTimestamp) {
                                        return element * 1000L;
                                    }
                                })
                                //TODO 指定空闲等待时间，超过这个时间没有更新，就不去计算它了
                                .withIdleness(Duration.ofSeconds(5))
                );

        sensorDS.keyBy(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value % 2;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        out.collect("key = " + integer + "的窗口[" + windowStart + "，" + windowEnd + "，包含了" + elements.spliterator().estimateSize() + "条数据：" + elements);
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
