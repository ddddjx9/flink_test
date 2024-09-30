package cn.edu.ustb.processFunction;

import cn.edu.ustb.processFunction.function.ProcessFunctionTopN;
import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFunctionTopN_KeyedProcessFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //TODO URL：http://localhost:8081
        env.setParallelism(1);

        env.socketTextStream("localhost", 8888)
                .map(value -> {
                    String[] split = value.split(" ");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(3)
                                )
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000L
                                )
                )
                .keyBy(WaterSensor::getVc)
                //TODO 统计一段时间内出现最多的水位，也就是10s窗口内的水位，每隔5s输出一次，所以需要打标签区分是哪个窗口
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        //TODO 增量聚合
                        new AggregateFunction<WaterSensor, Integer, Integer>() {
                            @Override
                            public Integer createAccumulator() {
                                return 0;
                            }

                            @Override
                            public Integer add(WaterSensor value, Integer accumulator) {
                                return accumulator + 1;
                            }

                            @Override
                            public Integer getResult(Integer accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Integer merge(Integer a, Integer b) {
                                return a + b;
                            }
                        }
                        ,
                        //TODO 打标签，开窗聚合后，就是普通的流，没有窗口信息，所以需要打标签
                        /*
                          IN：输入类型为增量函数的输出类型（Integer）
                          OUT：输出类型，VC，Count，窗口结束时间戳（标签），Tuple3
                          KEY：按照水位线分组，Integer
                          窗口类型：时间窗口
                         */
                        new ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>() {

                            /**
                             * 对窗口增量后的结果进行打标签操作
                             * @param key The key for which this window is evaluated.
                             * @param context The context in which the window is being evaluated.
                             * @param elements The elements in the window being evaluated.
                             * @param out A collector for emitting elements.
                             */
                            @Override
                            public void process(Integer key, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) {
                                //TODO 迭代器里面只有一条数据
                                Integer count = elements.iterator().next();
                                out.collect(new Tuple3<>(key, count, context.window().getEnd()));
                            }
                        }
                )
                //TODO 按照窗口结束时间keyBy，让同一个窗口的数据进行排序
                .keyBy((KeySelector<Tuple3<Integer, Integer, Long>, Long>) value -> value.f2)
                //这里只能写有名字的类，否则无法创建动态传参的构造器
                .process(new ProcessFunctionTopN(3))
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
