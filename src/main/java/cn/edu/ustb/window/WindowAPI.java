package cn.edu.ustb.window;

import cn.edu.ustb.sourceOperator.WaterSensor;
import cn.edu.ustb.transformOperator.userDefinedFunction.MyMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowAPI {
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
        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10L))); //滚动
        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10L), Time.seconds(2L))); //滑动，指定步长
        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5L))); //中间间隔多久没有数据来 - 会话

        //TODO       基于数据的窗口 - 窗口长度100个元素，每100个元素一个窗口
        sensorKS.countWindow(100);  //滚动窗口
        sensorKS.countWindow(10,2);  //窗口容量为10，滑动步长为2个元素
        sensorKS.window(GlobalWindows.create());  //全局窗口，计数窗口的底层逻辑

        //TODO 2.指定窗口函数 - 定义窗口计算逻辑

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
