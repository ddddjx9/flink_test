package cn.edu.ustb.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformOperator_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6);
        source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                RuntimeContext runtimeContext = getRuntimeContext();
                System.out.println("子任务编号：" + runtimeContext.getIndexOfThisSubtask() +
                        "启动，task名称为" + runtimeContext.getTaskNameWithSubtasks()+", 调用open");
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        }).print();

        env.execute();
    }
}
