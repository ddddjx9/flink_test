package cn.edu.ustb.transformOperator.userDefinedFunction;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class MyMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String s) throws Exception {
        String[] split = s.split(" ");
        return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
    }
}
