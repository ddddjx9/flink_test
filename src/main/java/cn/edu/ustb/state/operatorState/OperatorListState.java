package cn.edu.ustb.state.operatorState;

import cn.edu.ustb.state.operatorState.function.MyCountMapFunction_ListState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorListState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
                //TODO map算子本身就是无状态算子，如果想在里面定义状态，必须实现对应接口
                .map(new MyCountMapFunction_ListState())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
