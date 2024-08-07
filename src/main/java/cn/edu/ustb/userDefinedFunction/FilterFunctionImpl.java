package cn.edu.ustb.userDefinedFunction;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

public class FilterFunctionImpl implements FilterFunction<WaterSensor> {

    private String id;

    public FilterFunctionImpl(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return false;
    }
}
