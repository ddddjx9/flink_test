package cn.edu.ustb.transformOperator.unionStream;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> source2 = env.fromElements("11", "22", "44", "45");

        ConnectedStreams<Integer, String> connect = source1.connect(source2);//一次只能够传递一个参数
        connect.map(new CoMapFunction<Integer, String, Integer>() {
            @Override
            public Integer map1(Integer value) throws Exception {
                return value;
            }

            @Override
            public Integer map2(String value) throws Exception {
                return Integer.parseInt(value);
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
