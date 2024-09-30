package cn.edu.ustb.processFunction;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * 实时统计一段时间内的出现次数最多的水位
 * <p>
 * 最终汇总排序后输出前两名
 * </p>
 */
public class ProcessFunctionTopN_AllWindowedStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
                .map((MapFunction<String, WaterSensor>) value -> {
                    String[] split = value.split(" ");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(3)
                                )
                                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000L)
                )
                .keyBy(WaterSensor::getId)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) {
                        HashMap<Integer, Integer> vcMap = new HashMap<>();
                        for (WaterSensor element : elements) {
                            if (vcMap.containsKey(element.getVc())) {
                                vcMap.put(element.getVc(), vcMap.get(element.getVc()) + 1);
                            } else {
                                vcMap.put(element.getVc(), 1);
                            }
                        }

                        List<Tuple2<Integer,Integer>> result=new ArrayList<>();
                        Set<Integer> integers = vcMap.keySet();
                        for (Integer integer : integers) {
                            result.add(new Tuple2<>(integer,vcMap.get(integer)));
                        }

                        result.sort((o1, o2) -> o2.f1-o1.f1);

                        for (int i = 0; i <2; i++) {
                            out.collect(result.get(i).f0+"水位线出现了："+result.get(i).f1+"次");
                        }
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
