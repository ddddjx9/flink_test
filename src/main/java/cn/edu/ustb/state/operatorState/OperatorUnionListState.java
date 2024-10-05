package cn.edu.ustb.state.operatorState;

import cn.edu.ustb.state.operatorState.function.MyCountMapFunction_UnionListState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p>
 *     算子状态中，List与UnionList的区别：
 * </p>
 * <p>
 *     并行度改变时，怎么重新分配状态
 * </p>
 */
public class OperatorUnionListState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
                //TODO map算子本身就是无状态算子，如果想在里面定义状态，必须实现对应接口
                .map(new MyCountMapFunction_UnionListState())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
