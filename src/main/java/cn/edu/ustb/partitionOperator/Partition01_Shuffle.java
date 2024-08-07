package cn.edu.ustb.partitionOperator;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Partition01_Shuffle {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("Hadoop132", 7777);

        //随机分区
        //source.shuffle().print();

        //轮询
        //source.rebalance().print();

        //缩放
        source.rescale().print();

        //广播
        source.broadcast().print();

        //全局
        source.global().print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
