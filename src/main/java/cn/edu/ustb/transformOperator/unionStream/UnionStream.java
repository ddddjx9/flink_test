package cn.edu.ustb.transformOperator.unionStream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 演示多条数据流合流的操作
 */
public class UnionStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> source2 = env.fromElements(11, 22, 33, 44, 55, 66);
        DataStreamSource<String> source3 = env.fromElements("1", "2", "44", "45");

        source1.union(source2)
                .union(source3.map(Integer::parseInt))
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
