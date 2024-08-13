package cn.edu.ustb.window;

import cn.edu.ustb.sourceOperator.WaterSensor;
import cn.edu.ustb.transformOperator.userDefinedFunction.MyMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowFunction_ProcessWindowFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<WaterSensor, String> sensorKS = env.socketTextStream("localhost", 9999)
                .map(new MyMapFunction())
                .keyBy(WaterSensor::getId);

        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10L)));

        sensorWS
//                .apply(new WindowFunction<WaterSensor, String, String, TimeWindow>() {
//            /**
//             * 全窗口函数 - 将所有数据攒着统一进行处理
//             * @param s The key for which this window is evaluated.
//             * @param window The window that is being evaluated.
//             * @param input The elements in the window being evaluated.
//             * @param out A collector for emitting elements.
//             */
//            @Override
//            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<String> out) {
//
//            }
//        });
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    /**
                     * 窗口函数的处理
                     * @param s 分组的key
                     * @param context 上下文
                     * @param elements 可迭代的一堆数据
                     * @param out 数据采集器
                     */
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = elements.spliterator().estimateSize();
                        out.collect("key = "+s+"的窗口["+windowStart+", "+windowEnd+"]包含了"+count+"条数据："+elements);
                    }
                })
                //整个窗口只输出了一次，并不是来一条计算一条
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
