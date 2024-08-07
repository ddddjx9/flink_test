package cn.edu.ustb.transformOperator;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformOperator03_FlatMap {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> sensors = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 1L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        sensors.flatMap(new FlatMapFunction<WaterSensor, String>() {
                    @Override
                    public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                        if("s1".equals(value.getId())) {
                            System.out.println("vc: "+value.getVc());
                            out.collect(String.valueOf(value.getVc()));
                        }else if("s2".equals(value.getId())){
                            System.out.println("vc: "+value.getVc());
                            System.out.println("ts: "+value.getTs());
                            out.collect(value.getVc()+", "+value.getTs());
                        }
                    }
                });
                //.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
