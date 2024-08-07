package cn.edu.ustb.partitionOperator;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Partition02_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        source.partitionCustom(new MyPartitioner(),value -> value)
                .print();

        env.execute();
    }
}
