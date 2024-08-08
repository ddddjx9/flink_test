package cn.edu.ustb.transformOperator.splitStream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        source.filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return Integer.parseInt(value) % 2 == 0;
                    }
                })
                .print("偶数流：");

        source.filter(value -> Integer.parseInt(value) % 2 != 0).print("奇数流：");

        env.execute();
    }
}
