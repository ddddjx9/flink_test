package cn.edu.ustb.transformOperator;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformOperator01_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<WaterSensor> sensors = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        sensors.map(new MapFunction<WaterSensor, String>() {
                    @Override
                    public String map(WaterSensor value) throws Exception {
                        return value.getId();
                    }
                })
                //更方便的话，可以定义一个类，相同的逻辑只需要书写一遍，比如方法引用或者直接new一个类
                .print();

        env.execute();
    }
}
