package cn.edu.ustb.userDefinedFunction;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

public class MyFilterFunction implements FilterFunction<WaterSensor> {

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return "s1".equals(value.getId());
    }
}
