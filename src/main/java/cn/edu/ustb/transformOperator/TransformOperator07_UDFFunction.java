package cn.edu.ustb.transformOperator;

import cn.edu.ustb.sourceOperator.WaterSensor;
import cn.edu.ustb.userDefinedFunction.FilterFunctionImpl;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformOperator07_UDFFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensors = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 13L, 11),
                new WaterSensor("s1", 13L, 12),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        sensors.//filter(new MyFilterFunction())
                filter(new FilterFunctionImpl("s2"))
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
