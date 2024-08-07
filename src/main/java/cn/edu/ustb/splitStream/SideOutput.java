package cn.edu.ustb.splitStream;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 使用侧输出流实现分流
 * <p>
 * 将WaterSensor按照ID类型进行分流：
 * </p>
 */
public class SideOutput {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 9999)
                .map(value -> {
                            String[] data = value.split(",");
                            return new WaterSensor(data[0], Long.parseLong(data[1]), Integer.parseInt(data[2]));
                        }
                );

        sensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) {
                //最后定义的是主流的输出类型，不是侧流的输出类型
                if ("s1".equals(value.getId())) {
                    //如果是s1，就放到侧输出流s1中
                    ctx.output(new OutputTag<>("s1"), value); //命名小支流
                } else if ("s2".equals(value.getId())) {
                    //如果是s2，放到侧输出流s2中
                    ctx.output(new OutputTag<>("s2"), value);
                } else {
                    //其他的在主流中
                    out.collect(value);
                }
            }
        });

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
