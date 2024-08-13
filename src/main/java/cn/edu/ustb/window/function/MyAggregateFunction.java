package cn.edu.ustb.window.function;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 第一个类型：输入数据类型
 * <p>
 * 第二个类型：中间累加器类型
 * </p>
 * <p>
 * 第三个类型：输出数据类型
 */
public class MyAggregateFunction implements AggregateFunction<WaterSensor, Integer, String> {
    /**
     * 属于本窗口的第一条数据来，创建窗口，创建累加器
     * @return 返回一个初始化好的累加器
     */
    @Override
    public Integer createAccumulator() {
        System.out.println("创建累加器");
        return 0;
    }

    /**
     * 累加的计算逻辑，来一条计算一条，调用一次add方法
     * @param value The value to add
     * @param accumulator The accumulator to add the value to
     * @return 返回一个累加后的值
     */
    @Override
    public Integer add(WaterSensor value, Integer accumulator) {
        System.out.println("调用add方法"+value);
        return value.getVc() + accumulator;
    }

    /**
     * 返回最终的聚合结果 - 窗口触发时输出，调用一次getResult方法
     * @param accumulator The accumulator of the aggregation
     * @return 返回最终累加完毕之后的输出类型 - 字符串
     */
    @Override
    public String getResult(Integer accumulator) {
        System.out.println("调用getResult方法：");
        return "各个水位器累加后的结果为：" + accumulator;
    }

    /**
     * 合并多个节点中累加器中的结果
     * @param a An accumulator to merge
     * @param b Another accumulator to merge
     * @return 返回两个累加器合并之后的结果
     */
    @Override
    public Integer merge(Integer a, Integer b) {
        //只有会话窗口才会用到
        System.out.println("调用merge方法：");
        return Integer.sum(a, b);
    }
}
