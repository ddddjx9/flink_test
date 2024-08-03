package cn.edu.ustb.sourceOperator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkSource01_Collection {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.//fromCollection(Arrays.asList(1,2,9,7,6,8,4,3)).print();
        fromElements(1,3,6,2,7,4);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
