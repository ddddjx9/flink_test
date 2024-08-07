package cn.edu.ustb.transformOperator;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformOperator05_SimpleReduce {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> sensors = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 13L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> keyByDS = sensors.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        /*keyByDS
                //Exception in thread "main" org.apache.flink.api.common.typeutils.CompositeType$InvalidFieldReferenceException:
                //Cannot reference field by position on PojoType<cn.edu.ustb.sourceOperator.WaterSensor
                .sum("vc")
                .print();*/

        //keyByDS.max("vc").print();

        keyByDS.maxBy("vc").print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
