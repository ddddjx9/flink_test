package cn.edu.ustb.state;

import cn.edu.ustb.sourceOperator.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

public class KeyedListState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> mainStream = env.socketTextStream("localhost", 8888)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(3)
                                )
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000L;
                                    }
                                })
                )
                .keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            //TODO 定义状态，此时不进行初始化
                            ListState<Integer> state;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                //TODO 在open方法中初始化状态
                                super.open(parameters);
                                //TODO 通过运行时上下文获取状态，通过状态描述器定义状态
                                state = getRuntimeContext().getListState(new ListStateDescriptor<>("listState", Types.INT));
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                //TODO 来一条，存一条
                                state.add(value.getVc());
                                Iterable<Integer> iterator = state.get();

                                //TODO 从迭代器中拿出来，拷贝到list中，只留三个最大的
                                ArrayList<Integer> list = new ArrayList<>();
                                for (Integer i : iterator) {
                                    list.add(i);
                                }

                                //TODO 降序排序
                                list.sort((o1, o2) -> o2 - o1);

                                //TODO 只保留最大的三个
                                if (list.size() > 3) {
                                    list.remove(3);
                                }

                                out.collect("传感器id为："+value.getId()+"，最大的三个水位值 = "+ list);

                                //TODO 更新list状态
                                state.update(list);
                            }
                        });

        mainStream.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
