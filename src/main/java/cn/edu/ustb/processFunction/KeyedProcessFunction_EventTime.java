package cn.edu.ustb.processFunction;

import cn.edu.ustb.sourceOperator.WaterSensor;
import cn.edu.ustb.transformOperator.userDefinedFunction.MyMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedProcessFunction_EventTime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //env.getConfig().setAutoWatermarkInterval();

        //TODO 在算子外部指定Watermark策略
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy =
                WatermarkStrategy
                        //乱序的watermark策略，需要指定等待时间 - 指定等待3s时间
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        //指定时间戳分配器，从数据中提取
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                                    System.out.println(element + " ,recordTs = " + recordTimestamp);
                                    return element.getTs() * 1000L;
                                }
                        );

        env.socketTextStream("localhost", 7777)
                .map(new MyMapFunction())
                .assignTimestampsAndWatermarks(waterSensorWatermarkStrategy)
                .keyBy((KeySelector<WaterSensor, String>) WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    /**
                     * 来一条数据处理一次，调用一次该方法
                     * @param value 输入的数据
                     * @param ctx 上下文 {@link Context} that allows querying the timestamp of the element and getting a
                     *     {@link TimerService} for registering timers and querying the time. The context is only
                     *     valid during the invocation of this method, do not store it.
                     * @param out The collector for returning result values. 采集输出数据
                     */
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //TODO 数据中提取出来的事件时间
                        Long timestamp = ctx.timestamp();

                        //TODO 定时器
                        TimerService timerService = ctx.timerService();
                        //TODO 注册定时器：事件时间
                        timerService.registerEventTimeTimer(5 * 1000L);
                        System.out.println("当前key = " + ctx.getCurrentKey() + "当前时间是：" + timestamp + "，注册了一个5s的定时器");
                        //TODO 注册定时器：处理时间
                        //timerService.registerProcessingTimeTimer();

                        //TODO 删除定时器：处理时间
                        //timerService.deleteProcessingTimeTimer();

                        //timerService.currentWatermark();
                        //timerService.currentProcessingTime();
                    }

                    /**
                     * 时间进展到定时器注册的时间，调用该方法
                     * @param timestamp 当前时间进展
                     * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
                     *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
                     *     registering timers and querying the time. The context is only valid during the invocation
                     *     of this method, do not store it. 上下文
                     * @param out The collector for returning result values. 输出结果采集器
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        System.out.println("现在时间是：" + timestamp + "定时器触发");
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
