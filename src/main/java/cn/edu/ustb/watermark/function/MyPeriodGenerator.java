package cn.edu.ustb.watermark.function;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyPeriodGenerator implements WatermarkGenerator<WaterSensor> {

    //保存当前为止最大的事件时间
    private Long maxTimestamp;
    //最大的乱序时间
    private final Long outOfOrdernessMillis;

    public MyPeriodGenerator(long outOfOrdernessMillis) {
        this.outOfOrdernessMillis = outOfOrdernessMillis;
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    /**
     * 每条数据来，都会被调用一次，会提取最大的事件时间并保存
     *
     * @param event          每条来的数据/事件
     * @param eventTimestamp 每条数据所携带的事件时间
     * @param output         输出水位线
     */
    @Override
    public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        System.out.println("调用onEvent方法：最大时间戳 = " + maxTimestamp);
    }

    /**
     * 用来生成watermark
     *
     * @param output 周期性生成新的水位线
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        System.out.println("调用onPeriodicEmit方法：" + "水位线为" + (maxTimestamp - outOfOrdernessMillis - 1));
    }
}
