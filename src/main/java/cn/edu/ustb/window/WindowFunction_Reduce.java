package cn.edu.ustb.window;

import cn.edu.ustb.sourceOperator.WaterSensor;
import cn.edu.ustb.transformOperator.userDefinedFunction.MyMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowFunction_Reduce {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KeyedStream<WaterSensor, String> sensorKS = env.socketTextStream("localhost", 9999)
                .map(new MyMapFunction())
                .keyBy(WaterSensor::getId);

        //TODO 1.指定窗口分配器 - 指定用哪一种窗口
        //      1.1 没有keyBy的窗口 - 窗口内的所有数据会进入同一个子任务
        //      1.2 有keyBy的窗口 - 每个key上都定义了一组窗口，各自独立地进行统计计算。
        //          基于时间的窗口
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10L)));

        //TODO 2.指定窗口函数 - 定义窗口计算逻辑
        //      增量聚合：来一条数据，计算一条数据，但并不会立马输出 - 窗口触发的时候才会去计算和输出
        sensorWS.reduce(
                (ReduceFunction<WaterSensor>) (waterSensor, t1) -> {
                    System.out.println("调用reduce方法，value1：" + waterSensor + " value2：" + t1);
                    return new WaterSensor(waterSensor.getId(), waterSensor.getTs(), waterSensor.getVc() + t1.getVc());
                }
        ).print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
